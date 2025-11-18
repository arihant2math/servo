/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

// https://www.w3.org/TR/service-workers/#cache-interface

// https://www.w3.org/TR/service-workers/#cache-interface
[SecureContext, Exposed=(Window,Worker)]
interface Cache {
  // TODO: FrozenArray support
  [NewObject] Promise</*(Response or undefined)*/Response?> match(RequestInfo request, optional CacheQueryOptions options = {});
  [NewObject] Promise</*FrozenArray*/sequence<Response>> matchAll(optional RequestInfo request, optional CacheQueryOptions options = {});
  [NewObject] Promise<undefined> add(RequestInfo request);
  [NewObject] Promise<undefined> addAll(sequence<RequestInfo> requests);
  [NewObject] Promise<undefined> put(RequestInfo request, Response response);
  [NewObject] Promise<boolean> delete(RequestInfo request, optional CacheQueryOptions options = {});
  [NewObject] Promise</*FrozenArray*/sequence<Request>> keys(optional RequestInfo request, optional CacheQueryOptions options = {});
};

// https://www.w3.org/TR/service-workers/#dictdef-cachequeryoptions
dictionary CacheQueryOptions {
  boolean ignoreSearch = false;
  boolean ignoreMethod = false;
  boolean ignoreVary = false;
};
