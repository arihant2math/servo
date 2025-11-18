use std::rc::Rc;
use dom_struct::dom_struct;
use script_bindings::codegen::GenericBindings::CacheStorageBinding::{CacheStorageMethods, MultiCacheQueryOptions};
use script_bindings::codegen::GenericUnionTypes::RequestOrUSVString;
use script_bindings::domstring::DOMString;
use script_bindings::reflector::Reflector;
use script_bindings::root::DomRoot;
use script_bindings::script_runtime::CanGc;
use crate::dom::bindings::codegen::DomTypeHolder::DomTypeHolder;
use crate::dom::bindings::reflector::{reflect_dom_object, DomGlobal};
use crate::dom::globalscope::GlobalScope;
use crate::dom::promise::Promise;

#[dom_struct]
pub struct CacheStorage {
    reflector_: Reflector,
}

impl CacheStorage {
    fn new_inherited() -> CacheStorage {
        CacheStorage {
            reflector_: Reflector::new(),
        }
    }

    pub(crate) fn new(
        global: &GlobalScope,
        can_gc: CanGc,
    ) -> DomRoot<CacheStorage> {
        reflect_dom_object(
            Box::new(CacheStorage::new_inherited()),
            global,
            can_gc,
        )
    }
}

impl CacheStorageMethods<crate::DomTypeHolder> for CacheStorage {
    /// <https://www.w3.org/TR/service-workers/#cache-storage-match>
    fn Match(&self, request: RequestOrUSVString<DomTypeHolder>, options: &MultiCacheQueryOptions) -> Rc<Promise> {
        let promise = Promise::new(&*self.global(), CanGc::note());
        promise.resolve_native(&None, CanGc::note());
        promise
    }

    /// <https://www.w3.org/TR/service-workers/#cache-storage-has>
    fn Has(&self, cache_name: DOMString) -> Rc<Promise> {
        let promise = Promise::new(&*self.global(), CanGc::note());
        promise
    }

    /// <https://www.w3.org/TR/service-workers/#cache-storage-open>
    fn Open(&self, cache_name: DOMString) -> Rc<Promise> {
        let promise = Promise::new(&*self.global(), CanGc::note());
        promise
    }

    /// <https://www.w3.org/TR/service-workers/#cache-storage-delete>
    fn Delete(&self, cache_name: DOMString) -> Rc<Promise> {
        let promise = Promise::new(&*self.global(), CanGc::note());
        promise
    }

    /// <https://www.w3.org/TR/service-workers/#cache-storage-keys>
    fn Keys(&self) -> Rc<Promise> {
        let promise = Promise::new(&*self.global(), CanGc::note());
        promise
    }
}
