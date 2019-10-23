extern crate proc_macro;

use darling::FromMeta;
use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, AttributeArgs, ItemFn};

// #[derive(Debug, FromMeta)]
// struct TestEachConnectorArgs {
//     /// Comma-separated list of connectors to test.
//     #[darling(default)]
//     connectors: Option<String>,
//     #[darling(default)]
//     only: Option<String>,
// }

#[proc_macro_attribute]
pub fn test_each_connector(attr: TokenStream, input: TokenStream) -> TokenStream {
    // let attributes_meta: syn::AttributeArgs = parse_macro_input!(attr as AttributeArgs);
    // let args = TestEachConnectorArgs::from_list(&attributes_meta).unwrap();

    let test_function = parse_macro_input!(input as ItemFn);

    if test_function.sig.asyncness.is_none() {
        panic!("#[test_each_connector] works only with async test functions.");
    }

    let test_fn_name = test_function.sig.ident;
    let test_fn_impl_name = format!("__impl_{}", &test_fn_name);

    assert_eq!(
        test_function.sig.inputs.len(),
        1,
        "An async test with #[test_each_connector] only takes one argument."
    );

    quote! {
        #[test]
        fn #test_fn_name() {
            test_each_connector(#test_fn_impl_name)
        }
    }
}
