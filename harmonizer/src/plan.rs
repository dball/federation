/*!
# Create a query plan
*/

use deno_core::{op_sync, JsRuntime};
use serde::{Deserialize, Serialize};
use std::sync::mpsc::channel;
use std::{fmt::Display, io::Write};
use thiserror::Error;


#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
/// Options for the query plan
pub struct QueryPlanOptions {
    /// TODO
    pub auto_fragmentization: bool,

}

/// Default options for query planning
trait QueryPlanOptionsDefault {
    const DEFAULT: Self;
}

/// Default options for query planning
impl QueryPlanOptionsDefault for QueryPlanOptions {
    const DEFAULT : QueryPlanOptions = QueryPlanOptions {
        auto_fragmentization : false
    };
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
/// The query to be planned
pub struct OperationalContext {
    /// The graphQL schema
    pub schema: String,
    /// The query
    pub query: String,
    /// The operation
    pub operation: String,
}


/// An error which occurred during JavaScript planning.
///
/// The shape of this error is meant to mimick that of the error created within
/// JavaScript, which is a [`GraphQLError`] from the [`graphql-js`] library.
///
/// [`graphql-js']: https://npm.im/graphql
/// [`GraphQLError`]: https://github.com/graphql/graphql-js/blob/3869211/src/error/GraphQLError.js#L18-L75
#[derive(Debug, Error, Serialize, Deserialize, PartialEq)]
pub struct PlanningError {
    /// A human-readable description of the error that prevented planning.
    pub message: Option<String>,
    /// [`planningErrorExtensions`]
    pub extensions: Option<PlanningErrorExtensions>,
}

impl Display for PlanningError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(msg) = &self.message {
            f.write_fmt(format_args!("{code}: {msg}", code = self.code(), msg = msg))
        } else {
            f.write_str(self.code())
        }
    }
}


#[derive(Debug, Serialize, Deserialize, PartialEq)]
/// Errors
pub struct PlanningErrorExtensions {
    /// The error code
    pub code: String,
}

/// An error that was received during planning within JavaScript.
impl PlanningError {
    /// Retrieve the error code from an error received during planning.
    pub fn code(&self) -> &str {
        match self.extensions {
            Some(ref ext) => &*ext.code,
            None => "UNKNOWN",
        }
    }
}

/// Create the query plan by calling in to JS.
///
pub fn plan(context: OperationalContext, options: QueryPlanOptions) -> Result<String, Vec<PlanningError>> {
    // Initialize a runtime instance
    let mut runtime = JsRuntime::new(Default::default());

    // We'll use this channel to get the results
    let (tx, rx) = channel();

    // The first thing we do is define an op so we can print data to STDOUT,
    // because by default the JavaScript console functions are just stubs (they
    // don't do anything).

    // Register the op for outputting bytes to stdout. It can be invoked with
    // Deno.core.dispatch and the id this method returns or
    // Deno.core.dispatchByName and the name provided.
    runtime.register_op(
        "op_print",
        // The op_fn callback takes a state object OpState,
        // a structured arg of type `T` and an optional ZeroCopyBuf,
        // a mutable reference to a JavaScript ArrayBuffer
        op_sync(|_state, _msg: Option<String>, zero_copy| {
            let mut out = std::io::stdout();

            // Write the contents of every buffer to stdout
            if let Some(buf) = zero_copy {
                out.write_all(&buf)
                    .expect("failure writing buffered output");
            }

            Ok(()) // No meaningful result
        }),
    );

    runtime.register_op(
        "op_composition_result",
        op_sync(move |_state, value, _zero_copy| {
            tx.send(serde_json::from_value(value).expect("deserializing composition result"))
                .expect("channel must be open");

            Ok(serde_json::json!(null))

            // Don't return anything to JS
        }),
    );

    // The runtime automatically contains a Deno.core object with several
    // functions for interacting with it.
    runtime
        .execute(
            "<init>",
            include_str!("../js/runtime.js"),
        )
        .expect("unable to initialize bridge runtime environment");

    // Load the composition library.
    runtime
        .execute("bridge.js", include_str!("../dist/bridge.js"))
        .expect("unable to evaluate bridge module");

    // We literally just turn it into a JSON object that we'll execute within
    // the runtime.
    let context_javascript = format!(
        "context = {}",
        serde_json::to_string(&context)
            .expect("unable to serialize query plan context into JavaScript runtime")
    );

    let options_javascript = format!(
        "options = {}",
        serde_json::to_string(&options)
            .expect("unable to serialize query plan options list into JavaScript runtime")
    );

    runtime
        .execute("<set_context>", &context_javascript)
        .expect("unable to evaluate service list in JavaScript runtime");

    runtime
        .execute("<set_options>", &options_javascript)
        .expect("unable to evaluate service list in JavaScript runtime");

    runtime
        .execute("do_plan.js", include_str!("../js/do_plan.js"))
        .expect("unable to invoke do_plan in JavaScript runtime");

    rx.recv().expect("channel remains open")
}

#[cfg(test)]
mod tests {
    use super::*;
    const SCHEMA : &str = include_str!("testdata/schema.graphql");
    const QUERY : &str = include_str!("testdata/query.graphql");

    #[test]
    fn it_works() {
        insta::assert_snapshot!(plan(OperationalContext {
            schema: SCHEMA.to_string(),
            query: QUERY.to_string(),
            operation: "".to_string()
        },
        QueryPlanOptions::DEFAULT
        ).unwrap());
    }

    #[test]
    fn invalid_schema_is_caught() {
        let result = Err(vec![PlanningError{
            message: Some("Syntax Error: Unexpected Name \"Garbage\".".to_string()),
            extensions: None
        }]);
        assert_eq!(result, plan(OperationalContext {
            schema: "Garbage".to_string(),
            query: QUERY.to_string(),
            operation: "".to_string()
        },
        QueryPlanOptions::DEFAULT
        ));
    }

    #[test]
    fn invalid_query_is_caught() {
        let result = Err(vec![PlanningError{
            message: Some("Syntax Error: Unexpected Name \"Garbage\".".to_string()),
            extensions: None
        }]);
        assert_eq!(result, plan(OperationalContext {
            schema: SCHEMA.to_string(),
            query: "Garbage".to_string(),
            operation: "".to_string()
        },
                                QueryPlanOptions::DEFAULT
        ));
    }
}
