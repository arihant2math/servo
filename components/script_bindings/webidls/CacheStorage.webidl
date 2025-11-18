/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

// https://www.w3.org/TR/service-workers/#cachestorage-interface

// https://www.w3.org/TR/service-workers/#cachestorage-interface
[SecureContext, Exposed=(Window,Worker)]
interface CacheStorage {
  [NewObject] Promise<(Response or undefined)> match(RequestInfo request, optional MultiCacheQueryOptions options = {});
  [NewObject] Promise<boolean> has(DOMString cacheName);
  [NewObject] Promise<Cache> open(DOMString cacheName);
  [NewObject] Promise<boolean> delete(DOMString cacheName);
  [NewObject] Promise<sequence<DOMString>> keys();
};

// https://www.w3.org/TR/service-workers/#dictdef-multicachequeryoptions
dictionary MultiCacheQueryOptions : CacheQueryOptions {
  DOMString cacheName;
};

// https://www.w3.org/TR/service-workers/#self-caches
partial interface mixin WindowOrWorkerGlobalScope {
  [SecureContext, SameObject] readonly attribute CacheStorage caches;
};
