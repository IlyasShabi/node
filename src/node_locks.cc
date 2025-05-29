#include "node_locks.h"

#include "env-inl.h"
#include "node_errors.h"
#include "node_external_reference.h"
#include "node_internals.h"
#include "util-inl.h"
#include "v8.h"

using node::errors::TryCatchScope;
using v8::Array;
using v8::Context;
using v8::Exception;
using v8::External;
using v8::Function;
using v8::FunctionCallbackInfo;
using v8::HandleScope;
using v8::Isolate;
using v8::Local;
using v8::Object;
using v8::ObjectTemplate;
using v8::Promise;
using v8::String;
using v8::Value;

namespace node {
namespace worker {
namespace locks {

static constexpr const char* kSharedMode = "shared";
static constexpr const char* kExclusiveMode = "exclusive";
static constexpr const char* kLockStolenError = "LOCK_STOLEN";

static Local<Object> BuildLockObject(Isolate* isolate,
                                     Local<Context> context,
                                     const std::u16string& name,
                                     Lock::Mode mode,
                                     const std::string& client_id);

Lock::Lock(Environment* env,
           const std::u16string& name,
           Mode mode,
           const std::string& client_id,
           Local<Promise::Resolver> waiting,
           Local<Promise::Resolver> released)
    : env_(env), name_(name), mode_(mode), client_id_(client_id) {
  waiting_promise_.Reset(env_->isolate(), waiting);
  released_promise_.Reset(env_->isolate(), released);
}

Lock::~Lock() {
  waiting_promise_.Reset();
  released_promise_.Reset();
}

LockRequest::LockRequest(Environment* env,
                         Local<Promise::Resolver> waiting,
                         Local<Promise::Resolver> released,
                         Local<Function> callback,
                         const std::u16string& name,
                         Lock::Mode mode,
                         const std::string& client_id,
                         bool steal,
                         bool if_available)
    : env_(env), name_(name), mode_(mode), client_id_(client_id), steal_(steal), if_available_(if_available) {
  waiting_promise_.Reset(env_->isolate(), waiting);
  released_promise_.Reset(env_->isolate(), released);
  callback_.Reset(env_->isolate(), callback);
}

LockRequest::~LockRequest() {
  waiting_promise_.Reset();
  released_promise_.Reset();
  callback_.Reset();
}

bool LockManager::IsGrantable(const LockRequest* request) const {
  // Steal requests bypass all normal granting rules
  if (request->steal()) return true;

  auto held_it = held_locks_.find(request->name());
  // No existing locks for this resource name
  if (held_it == held_locks_.end()) return true;

  // Exclusive requests cannot coexist with any existing locks
  if (request->mode() == Lock::kExclusive) return false;

  // For shared requests, check if any existing lock is exclusive
  for (const auto& lock : held_it->second) {
    if (lock->mode() == Lock::kExclusive) return false;
  }
  // All existing locks are shared, so this shared request can be granted
  return true;
}

// Called when the user callback settles
static void OnSettledCallback(const v8::FunctionCallbackInfo<v8::Value>& info) {
  HandleScope handle_scope(info.GetIsolate());
  Environment* env = Environment::GetCurrent(info);
  
  auto* lock_slot = static_cast<std::shared_ptr<Lock>*>(
      info.Data().As<External>()->Value());
  std::shared_ptr<Lock> lock = *lock_slot;
  delete lock_slot;

  // Release the lock and continue processing the queue.
  LockManager::GetCurrent()->ReleaseLockAndProcessQueue(env, lock, info[0]);
}

/**
 * Remove stolen locks from other environments
 * and let stolen locks from the current environment complete naturally
 */
void LockManager::CleanupStolenLocks(Environment* env) {
  Mutex::ScopedLock lock(mutex_);
  
  for (auto it = held_locks_.begin(); it != held_locks_.end();) {
    auto& locks = it->second;
    for (auto lock_it = locks.begin(); lock_it != locks.end();) {
      // Only remove stolen locks from other environments
      if ((*lock_it)->is_stolen() && (*lock_it)->env() != env) {
        lock_it = locks.erase(lock_it);
      } else {
        ++lock_it;
      }
    }
    if (locks.empty()) {
      it = held_locks_.erase(it);
    } else {
      ++it;
    }
  }
}

/**
 * https://w3c.github.io/web-locks/#algorithms
 */
void LockManager::ProcessQueue(Environment* env) {
  Isolate* isolate = env->isolate();
  HandleScope handle_scope(isolate);
  Local<Context> context = env->context();
  
  // Remove locks that were stolen from this Environment first
  CleanupStolenLocks(env);

  while (true) {
    std::unique_ptr<LockRequest> grantable_request;
    std::unique_ptr<LockRequest> ifavailable_request;
    std::unordered_set<Environment*>
        other_envs_to_wake;  // related to pending requests

    {
      // Scan pending queue to find a request that can be granted
      Mutex::ScopedLock lock(mutex_);
      for (auto it = pending_queue_.begin(); it != pending_queue_.end(); ++it) {
        LockRequest* request = it->get();
        
        // Skip requests from other Environments, but wake them later
        if (request->env() != env) {
          other_envs_to_wake.insert(request->env());
          continue;
        }
        
        /**
         * A request for the same resource must wait until all earlier
         * requests are settled.
         */
        bool has_earlier_request_for_same_resource = false;
        for (auto it2 = pending_queue_.begin(); it2 != it; ++it2) {
          if ((*it2)->name() == request->name()) {
            has_earlier_request_for_same_resource = true;
            break;
          }
        }

        if (has_earlier_request_for_same_resource || !IsGrantable(request)) {
          if (request->if_available()) {
            // ifAvailable request when resource not available: grant with null
            ifavailable_request = std::move(*it);
            pending_queue_.erase(it);
            break;
          }
          continue;
        }

        // Found a request that can be granted normally
        grantable_request = std::move(*it);
        pending_queue_.erase(it);
        break;
      }
    }

    // Wake other environments so they can process their own queues
    for (Environment* target_env : other_envs_to_wake) {
      if (target_env != env) WakeEnvironment(target_env);
    }

    /**
     * ifAvailable:
     *  Grant the lock only if it is immediately available;
     *  otherwise invoke the callback with null and resolve the promises.
     *  Check wrapCallback function in locks.js
     */
    if (ifavailable_request) {
      Local<Value> null_arg = Null(isolate);
      Local<Value> cb_ret;
      {
        TryCatchScope tc(env);
        if (!ifavailable_request->callback()
                 ->Call(context, Undefined(isolate), 1, &null_arg)
                 .ToLocal(&cb_ret)) {
          ifavailable_request->waiting_promise()->Reject(context, tc.Exception()).Check();
          ifavailable_request->released_promise()->Reject(context, tc.Exception()).Check();
          return;
        }
      }
      ifavailable_request->waiting_promise()->Resolve(context, cb_ret).Check();
      ifavailable_request->released_promise()->Resolve(context, cb_ret).Check();
      return;
    }

    if (!grantable_request) return;

    if (grantable_request->steal()) {
      Mutex::ScopedLock lock(mutex_);
      auto held_it = held_locks_.find(grantable_request->name());
      if (held_it != held_locks_.end()) {
        std::unordered_set<Environment*> envs_to_notify; // to cleanup of stolen locks
        
        /**
         * If the lock is held by another Environment, mark it as stolen
         * and wake up the other Environment to clean it up.
         */
        for (auto& existing_lock : held_it->second) {
          existing_lock->mark_stolen();
          
          // Immediately reject the stolen lock's released_promise
          // This must happen regardless of whether the callback completes
          Local<Value> error = Exception::Error(
              String::NewFromUtf8(isolate, kLockStolenError).ToLocalChecked());
          existing_lock->released_promise()->Reject(context, error).Check();
          
          envs_to_notify.insert(existing_lock->env());
        }
        
        // Remove stolen locks from current environment immediately
        for (auto it = held_it->second.begin(); it != held_it->second.end();) {
          if ((*it)->env() == env) {
            it = held_it->second.erase(it);
          } else {
            ++it;
          }
        }
        
        if (held_it->second.empty()) {
          held_locks_.erase(held_it);
        }
        
        // Wake other environments to clean up their stolen locks
        for (Environment* target_env : envs_to_notify) {
          if (target_env != env) {
            WakeEnvironment(target_env);
          }
        }
      }
    }

    // Create and store the new granted lock
    auto granted_lock = std::make_shared<Lock>(env,
                                               grantable_request->name(),
                                               grantable_request->mode(),
                                               grantable_request->client_id(),
                                               grantable_request->waiting_promise(),
                                               grantable_request->released_promise());
    {
      Mutex::ScopedLock lock(mutex_);
      held_locks_[grantable_request->name()].push_back(granted_lock);
    }

    // Call user callback
    Local<Object> lock_obj = BuildLockObject(isolate, context, grantable_request->name(),
                                             grantable_request->mode(), grantable_request->client_id());
    Local<Value> arg = lock_obj;
    Local<Value> cb_ret;
    {
      TryCatchScope tc(env);
      if (!grantable_request->callback()->Call(context, Undefined(isolate), 1, &arg)
               .ToLocal(&cb_ret)) {
        grantable_request->waiting_promise()->Reject(context, tc.Exception()).Check();
        grantable_request->released_promise()->Reject(context, tc.Exception()).Check();
        continue;
      }
    }

    // Allocate a shared_ptr so the lock remains alive until the callback executes.
    auto* lock_holder = new std::shared_ptr<Lock>(granted_lock);
    Local<Function> onSettled = Function::New(
        context,
        OnSettledCallback,
        External::New(isolate, lock_holder))
        .ToLocalChecked();

    // Handle promise chain
    if (cb_ret->IsPromise()) {
      Local<Promise> promise = cb_ret.As<Promise>();
      if (promise->State() == Promise::kRejected) {
        Local<Value> rejection_value = promise->Result();
        grantable_request->waiting_promise()->Reject(context, rejection_value).Check();
        grantable_request->released_promise()->Reject(context, rejection_value).Check();
        delete lock_holder;
        {
          Mutex::ScopedLock lock(mutex_);
          ReleaseLock(granted_lock.get());
        }
        ProcessQueue(env);
        return;
      } else {
        grantable_request->waiting_promise()->Resolve(context, cb_ret).Check();
        USE(promise->Then(context, onSettled, onSettled));
      }
    } else {
      grantable_request->waiting_promise()->Resolve(context, cb_ret).Check();
      Local<Value> args[] = {cb_ret};
      USE(onSettled->Call(context, Undefined(isolate), 1, args));
    }
  }
}

/**
 * name        : string   – resource identifier
 * clientId    : string   – client identifier
 * mode        : string   – lock mode
 * steal       : boolean  – whether to steal existing locks
 * ifAvailable : boolean  – only grant if immediately available
 * callback    : Function - JS callback
 */
void LockManager::Request(const FunctionCallbackInfo<Value>& args) {
  Environment* env = Environment::GetCurrent(args);
  Isolate* isolate = env->isolate();
  HandleScope scope(isolate);
  Local<Context> context = env->context();

  CHECK_EQ(args.Length(), 6);
  CHECK(args[0]->IsString());
  CHECK(args[1]->IsString());
  CHECK(args[2]->IsString());
  CHECK(args[3]->IsBoolean());
  CHECK(args[4]->IsBoolean());
  CHECK(args[5]->IsFunction());

  Local<String> name = args[0].As<String>();
  TwoByteValue name_utf16(isolate, name);
  std::u16string name_str(reinterpret_cast<const char16_t*>(*name_utf16), name_utf16.length());
  String::Utf8Value client_id_utf8(isolate, args[1]);
  std::string client_id(*client_id_utf8);
  String::Utf8Value mode_utf8(isolate, args[2]);
  std::string mode_str(*mode_utf8);
  bool steal = args[3]->BooleanValue(isolate);
  bool if_available = args[4]->BooleanValue(isolate);
  Local<Function> callback = args[5].As<Function>();

  Lock::Mode mode = mode_str == kSharedMode ? Lock::kShared : Lock::kExclusive;

  Local<Promise::Resolver> waiting =
      Promise::Resolver::New(context).ToLocalChecked();
  Local<Promise::Resolver> released =
      Promise::Resolver::New(context).ToLocalChecked();

  args.GetReturnValue().Set(released->GetPromise());

  LockManager* manager = GetCurrent();
  {
    Mutex::ScopedLock lock(manager->mutex_);

    if (manager->registered_envs_.insert(env).second) {
      env->AddCleanupHook(LockManager::OnEnvironmentCleanup, env);
    }

    auto req = std::make_unique<LockRequest>(
        env, waiting, released, callback, name_str, mode, client_id, steal, if_available);
    if (steal) {
      manager->pending_queue_.emplace_front(std::move(req));
    } else {
      manager->pending_queue_.push_back(std::move(req));
    }
  }

  manager->ProcessQueue(env);
}

void LockManager::Query(const FunctionCallbackInfo<Value>& args) {
  Environment* env = Environment::GetCurrent(args);
  Isolate* isolate = env->isolate();
  HandleScope scope(isolate);
  Local<Context> context = env->context();

  Local<Promise::Resolver> resolver =
      Promise::Resolver::New(context).ToLocalChecked();
  args.GetReturnValue().Set(resolver->GetPromise());

  Local<Object> result = Object::New(isolate);
  Local<Array> held_list = Array::New(isolate);
  Local<Array> pending_list = Array::New(isolate);
  LockManager* manager = GetCurrent();

  {
    Mutex::ScopedLock lock(manager->mutex_);

    uint32_t index = 0;
    for (const auto& kv : manager->held_locks_) {
      for (const auto& held_lock : kv.second) {
        if (held_lock->env() == env) {
          Local<Object> obj = BuildLockObject(isolate, context, held_lock->name(),
                                             held_lock->mode(), held_lock->client_id());
          held_list->Set(context, index++, obj).Check();
        }
      }
    }

    index = 0;
    for (const auto& kv : manager->pending_queue_) {
      if (kv->env() == env) {
        Local<Object> obj = BuildLockObject(isolate, context, kv->name(),
                                           kv->mode(), kv->client_id());
        pending_list->Set(context, index++, obj).Check();
      }
    }
  }

  result->Set(context, FIXED_ONE_BYTE_STRING(isolate, "held"), held_list).Check();
  result->Set(context, FIXED_ONE_BYTE_STRING(isolate, "pending"), pending_list).Check();
  
  resolver->Resolve(context, result).Check();
}

// Runs after the user callback (or its returned promise) settles.
void LockManager::ReleaseLockAndProcessQueue(Environment* env, std::shared_ptr<Lock> lock, Local<Value> result) {
  {
    Mutex::ScopedLock lock_guard(mutex_);
    ReleaseLock(lock.get());
  }
  
  Local<Context> context = env->context();
  
  // For stolen locks, the released_promise was already rejected when marked as stolen
  // So we skip promise handling here
  if (!lock->is_stolen()) {
    if (result->IsPromise()) {
      Local<Promise> promise = result.As<Promise>();
      if (promise->State() == Promise::kFulfilled) {
        lock->released_promise()->Resolve(context, promise->Result()).Check();
      } else {
        lock->released_promise()->Reject(context, promise->Result()).Check();
      }
    } else {
      lock->released_promise()->Resolve(context, result).Check();
    }
  }
  
  ProcessQueue(env);
}

/**
 * Remove a lock from held_locks_ when it's no longer needed.
 * This makes the resource available for other waiting requests.
 */
void LockManager::ReleaseLock(Lock* lock) {
  const std::u16string& name = lock->name();
  auto it = held_locks_.find(name);
  if (it == held_locks_.end()) return;

  auto& locks = it->second;
  for (auto lock_it = locks.begin(); lock_it != locks.end(); ++lock_it) {
    if (lock_it->get() == lock) {
      locks.erase(lock_it);
      if (locks.empty()) held_locks_.erase(it);
      break;
    }
  }
}

// Wakeup of target Environment's event loop
void LockManager::WakeEnvironment(Environment* env) {
  if (env == nullptr || env->is_stopping()) return;

  env->SetImmediateThreadsafe([](Environment* target_env) {
    if (target_env != nullptr && !target_env->is_stopping()) {
      LockManager::GetCurrent()->ProcessQueue(target_env);
    }
  });
}

/**
 * Remove all held locks and pending requests that belong to an Environment
 * that is being destroyed.
 */
void LockManager::CleanupEnvironment(Environment* env) {
  Mutex::ScopedLock lock(mutex_);

  for (auto it = held_locks_.begin(); it != held_locks_.end();) {
    auto& dq = it->second;
    for (auto lock_it = dq.begin(); lock_it != dq.end();) {
      if ((*lock_it)->env() == env) {
        lock_it = dq.erase(lock_it);
      } else {
        ++lock_it;
      }
    }
    if (dq.empty()) {
      it = held_locks_.erase(it);
    } else {
      ++it;
    }
  }

  for (auto it = pending_queue_.begin(); it != pending_queue_.end();) {
    if ((*it)->env() == env) {
      it = pending_queue_.erase(it);
    } else {
      ++it;
    }
  }

  registered_envs_.erase(env);
}

// Cleanup hook wrapper
void LockManager::OnEnvironmentCleanup(void* arg) {
  Environment* env = static_cast<Environment*>(arg);
  LockManager::GetCurrent()->CleanupEnvironment(env);
}

static Local<Object> BuildLockObject(Isolate* isolate,
                                     Local<Context> context,
                                     const std::u16string& name,
                                     Lock::Mode mode,
                                     const std::string& client_id) {
  Local<Object> obj = Object::New(isolate);
  obj->Set(context,
           FIXED_ONE_BYTE_STRING(isolate, "name"),
           String::NewFromTwoByte(isolate,
                                  reinterpret_cast<const uint16_t*>(name.data()),
                                  v8::NewStringType::kNormal,
                                  static_cast<int>(name.length())).ToLocalChecked())
      .Check();
  obj->Set(context,
           FIXED_ONE_BYTE_STRING(isolate, "mode"),
           String::NewFromUtf8(
               isolate, mode == Lock::kExclusive ? kExclusiveMode : kSharedMode)
               .ToLocalChecked())
      .Check();
  obj->Set(context,
           FIXED_ONE_BYTE_STRING(isolate, "clientId"),
           String::NewFromUtf8(isolate, client_id.c_str()).ToLocalChecked())
      .Check();
  return obj;
}

LockManager LockManager::current_;

void CreatePerIsolateProperties(IsolateData* isolate_data,
                                Local<ObjectTemplate> target) {
  Isolate* isolate = isolate_data->isolate();
  SetMethod(isolate, target, "request", LockManager::Request);
  SetMethod(isolate, target, "query", LockManager::Query);
  
  // Expose constants to JavaScript
  target->Set(FIXED_ONE_BYTE_STRING(isolate, "LOCK_MODE_SHARED"),
              String::NewFromUtf8(isolate, kSharedMode).ToLocalChecked());
  target->Set(FIXED_ONE_BYTE_STRING(isolate, "LOCK_MODE_EXCLUSIVE"), 
              String::NewFromUtf8(isolate, kExclusiveMode).ToLocalChecked());
  target->Set(FIXED_ONE_BYTE_STRING(isolate, "LOCK_STOLEN_ERROR"),
              String::NewFromUtf8(isolate, kLockStolenError).ToLocalChecked());
}

void CreatePerContextProperties(Local<Object> target,
                                Local<Value> unused,
                                Local<Context> context,
                                void* priv) { }

void RegisterExternalReferences(ExternalReferenceRegistry* registry) {
  registry->Register(LockManager::Request);
  registry->Register(LockManager::Query);
}

}  // namespace locks
}  // namespace worker
}  // namespace node

NODE_BINDING_CONTEXT_AWARE_INTERNAL(
    locks, node::worker::locks::CreatePerContextProperties)
NODE_BINDING_PER_ISOLATE_INIT(locks,
                              node::worker::locks::CreatePerIsolateProperties)
NODE_BINDING_EXTERNAL_REFERENCE(locks,
                                node::worker::locks::RegisterExternalReferences)
