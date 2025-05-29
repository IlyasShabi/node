'use strict';

const locks = internalBinding('locks');
const { lazyDOMException } = require('internal/util');
const { threadId } = require('internal/worker');
const {
  ERR_INVALID_ARG_VALUE,
  ERR_ILLEGAL_CONSTRUCTOR,
} = require('internal/errors');
const {
  ObjectDefineProperties,
  Promise,
  PromiseResolve,
  Symbol,
  SymbolToStringTag,
  TypeError
} = primordials;

const kName = Symbol('kName');
const kMode = Symbol('kMode');
const kConstructLockManager = Symbol('kConstructLockManager');

// https://w3c.github.io/web-locks/#api-lock
class Lock {
  constructor (...args) {
    if (!args.length) {
      throw new ERR_ILLEGAL_CONSTRUCTOR();
    }
    this[kName] = args[0];
    this[kMode] = args[1];
  }

  get name () {
    if (this instanceof Lock) {
      return this[kName];
    }
    throw new TypeError('Illegal invocation');
  }

  get mode () {
    if (this instanceof Lock) {
      return this[kMode];
    }
    throw new TypeError('Illegal invocation');
  }
}

ObjectDefineProperties(Lock.prototype, {
  name: { enumerable: true },
  mode: { enumerable: true },
  [SymbolToStringTag]: {
    value: 'Lock',
    writable: false,
    enumerable: false,
    configurable: true,
  },
});

// https://w3c.github.io/web-locks/#api-lock-manager
class LockManager {
  constructor (...args) {
    if (args[0] !== kConstructLockManager) {
      throw new ERR_ILLEGAL_CONSTRUCTOR();
    }
  }

  // https://w3c.github.io/web-locks/#api-lock-manager-request
  async request (name, options, callback) {
    if (callback === undefined) {
      callback = options;
      options = undefined;
    }

    if (typeof name !== 'string') {
      throw new TypeError('The name argument must be a string');
    }

    if (typeof callback !== 'function') {
      throw new TypeError('The callback argument must be a function');
    }

    if (options !== undefined && (options === null || typeof options !== 'object')) {
      throw new TypeError('The options argument, when provided, must be an object');
    }

    // Set default options
    options = {
      mode: locks.LOCK_MODE_EXCLUSIVE,
      ifAvailable: false,
      steal: false,
      signal: undefined,
      ...options
    };

    if (name.startsWith('-')) {
      // If name starts with U+002D HYPHEN-MINUS (-), then reject promise with a
      // "NotSupportedError" DOMException.
      throw lazyDOMException('Lock name may not start with hyphen',
        'NotSupportedError');
    }

    if (options.ifAvailable === true && options.steal === true) {
      // If both options' steal dictionary member and option's
      // ifAvailable dictionary member are true, then reject promise with a
      // "NotSupportedError" DOMException.
      throw lazyDOMException('ifAvailable and steal are mutually exclusive',
        'NotSupportedError');
    }

    if (options.mode !== locks.LOCK_MODE_SHARED && options.mode !== locks.LOCK_MODE_EXCLUSIVE) {
      throw new ERR_INVALID_ARG_VALUE.TypeError(
        `mode must be "${locks.LOCK_MODE_SHARED}" or "${locks.LOCK_MODE_EXCLUSIVE}"`);
    }

    if (options.mode !== locks.LOCK_MODE_EXCLUSIVE && options.steal === true) {
      // If options' steal dictionary member is true and options' mode
      // dictionary member is not "exclusive", then return a promise rejected
      // with a "NotSupportedError" DOMException.
      throw lazyDOMException(`mode: "${locks.LOCK_MODE_SHARED}" and steal are mutually exclusive`,
        'NotSupportedError');
    }

    if (options.signal &&
      (options.steal === true || options.ifAvailable === true)) {
      // If options' signal dictionary member is present, and either of
      // options' steal dictionary member or options' ifAvailable dictionary
      // member is true, then return a promise rejected with a
      // "NotSupportedError" DOMException.
      throw lazyDOMException('signal cannot be used with steal or ifAvailable',
        'NotSupportedError');
    }

    if (options.signal !== undefined && !(options.signal instanceof AbortSignal)) {
      throw new TypeError('options.signal must be an AbortSignal');
    }

    if (options.signal && options.signal.aborted) {
      throw options.signal.reason || lazyDOMException('The operation was aborted', 'AbortError');
    }

    const clientId = `node-${process.pid}-${threadId}`;

    // Handle requests with AbortSignal
    function handleSignalRequest(name, clientId, options, callback) {
      return new Promise((resolve, reject) => {
        // Check for immediate abort
        if (options.signal.aborted) {
          reject(options.signal.reason ||
            lazyDOMException('The operation was aborted', 'AbortError'));
          return;
        }

        let aborted = false;
        let lockGranted = false;

        const abortListener = () => {
          aborted = true;
          // Only reject if the lock hasn't been granted yet
          if (!lockGranted) {
            reject(options.signal.reason ||
              lazyDOMException('The operation was aborted', 'AbortError'));
          }
        };

        options.signal.addEventListener('abort', abortListener, { once: true });

        try {
          // Wrap callback to check abort status and mark lock as granted
          const wrappedCallback = (lock) => {
            return Promise.resolve().then(() => {
              // If aborted before lock was granted, don't execute user callback
              if (aborted) {
                return undefined;
              }

              // Mark that the lock has been granted (abort signal no longer matters)
              lockGranted = true;

              return callback(createLock(lock));
            });
          };

          const released = locks.request(
            name,
            clientId,
            options.mode,
            options.steal,
            options.ifAvailable,
            wrappedCallback
          );

          // When released promise settles, clean up listener and resolve main promise
          released
            .then(resolve, (error) => {
              reject(convertLockError(error));
            })
            .finally(() => {
              options.signal.removeEventListener('abort', abortListener);
            });
        } catch (error) {
          options.signal.removeEventListener('abort', abortListener);
          reject(convertLockError(error));
        }
      });
    }

    if (options.signal) {
      return handleSignalRequest(name, clientId, options, callback);
    }

    // When ifAvailable: true and lock is not available, C++ passes null to indicate no lock granted
    const wrapCallback = (internalLock) => {
      const lock = internalLock === null ? null : new Lock(internalLock.name, internalLock.mode);
      return callback(lock);
    };

    // Standard request without signal
    try {
      return await locks.request(name, clientId, options.mode, options.steal, options.ifAvailable, wrapCallback);
    } catch (error) {
      throw convertLockError(error);
    }
  }

  // https://w3c.github.io/web-locks/#api-lock-manager-query
  async query () {
    if (this instanceof LockManager) {
      return locks.query();
    }
    throw new TypeError('Illegal invocation');
  }
}

ObjectDefineProperties(LockManager.prototype, {
  request: { enumerable: true },
  query: { enumerable: true },
  [SymbolToStringTag]: {
    value: 'LockManager',
    writable: false,
    enumerable: false,
    configurable: true,
  },
});

ObjectDefineProperties(LockManager.prototype.request, {
  length: {
    value: 2,
    writable: false,
    enumerable: false,
    configurable: true
  }
});

// Helper to create Lock objects from internal C++ lock data
function createLock(internalLock) {
  return internalLock === null ? null : new Lock(internalLock.name, internalLock.mode);
}

// Convert LOCK_STOLEN_ERROR to AbortError DOMException
function convertLockError (error) {
  if (error?.message === locks.LOCK_STOLEN_ERROR) {
    return lazyDOMException('The operation was aborted', 'AbortError');
  }
  return error;
}

module.exports = {
  Lock,
  LockManager,
  locks: new LockManager(kConstructLockManager)
};
