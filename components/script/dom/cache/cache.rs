use std::rc::Rc;
use dom_struct::dom_struct;
use script_bindings::codegen::GenericBindings::CacheBinding::{CacheMethods, CacheQueryOptions};
use script_bindings::codegen::GenericUnionTypes::RequestOrUSVString;
use script_bindings::domstring::DOMString;
use script_bindings::reflector::Reflector;
use script_bindings::script_runtime::CanGc;
use crate::dom::bindings::codegen::DomTypeHolder::DomTypeHolder;
use crate::dom::bindings::reflector::DomGlobal;
use crate::dom::promise::Promise;
use crate::dom::response::Response;

#[dom_struct]
pub struct Cache {
    reflector_: Reflector,
    name: DOMString,
}

impl CacheMethods<crate::DomTypeHolder> for Cache {
    fn Match(&self, request: RequestOrUSVString<DomTypeHolder>, options: &CacheQueryOptions) -> Rc<Promise> {
        let promise = Promise::new(&*self.global(), CanGc::note());
        promise
    }

    fn MatchAll(&self, request: Option<RequestOrUSVString<DomTypeHolder>>, options: &CacheQueryOptions) -> Rc<Promise> {
        let promise = Promise::new(&*self.global(), CanGc::note());
        promise
    }

    fn Add(&self, request: RequestOrUSVString<DomTypeHolder>) -> Rc<Promise> {
        let promise = Promise::new(&*self.global(), CanGc::note());
        promise
    }

    fn AddAll(&self, requests: Vec<RequestOrUSVString<DomTypeHolder>>) -> Rc<Promise> {
        let promise = Promise::new(&*self.global(), CanGc::note());
        promise
    }

    fn Put(&self, request: RequestOrUSVString<DomTypeHolder>, response: &Response) -> Rc<Promise> {
        let promise = Promise::new(&*self.global(), CanGc::note());
        promise
    }

    fn Delete(&self, request: RequestOrUSVString<DomTypeHolder>, options: &CacheQueryOptions) -> Rc<Promise> {
        let promise = Promise::new(&*self.global(), CanGc::note());
        promise
    }

    fn Keys(&self, request: Option<RequestOrUSVString<DomTypeHolder>>, options: &CacheQueryOptions) -> Rc<Promise> {
        let promise = Promise::new(&*self.global(), CanGc::note());
        promise
    }
}
