use std::collections::BTreeSet;
use std::fs;
use std::path::Path;

fn repo_file(path: &str) -> String {
    fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("..").join(path))
        .unwrap_or_else(|error| panic!("failed to read {path}: {error}"))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum QueryFamily {
    TypedMetadata,
    TypedJson,
    FullText,
    Vector,
    Hybrid,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum ScenarioInvariant {
    MultipleBoundaries,
    DistinctPrincipals,
    DeniedObjectPruned,
    PageTokenScopeBound,
    StaleAuthzRejected,
    StaleIndexRejected,
    PayloadReadAfterPlanner,
    CandidateMetricsRecorded,
}

#[derive(Debug)]
struct QueryPlannerE2eScenario {
    name: &'static str,
    family: QueryFamily,
    invariants: &'static [ScenarioInvariant],
}

const REQUIRED_SCENARIOS: &[QueryPlannerE2eScenario] = &[
    QueryPlannerE2eScenario {
        name: "metadata query prunes denied object before row payload materialisation",
        family: QueryFamily::TypedMetadata,
        invariants: &[
            ScenarioInvariant::MultipleBoundaries,
            ScenarioInvariant::DistinctPrincipals,
            ScenarioInvariant::DeniedObjectPruned,
            ScenarioInvariant::PageTokenScopeBound,
            ScenarioInvariant::StaleAuthzRejected,
            ScenarioInvariant::PayloadReadAfterPlanner,
            ScenarioInvariant::CandidateMetricsRecorded,
        ],
    },
    QueryPlannerE2eScenario {
        name: "typed json exact predicate uses value-index candidates and scoped page tokens",
        family: QueryFamily::TypedJson,
        invariants: &[
            ScenarioInvariant::MultipleBoundaries,
            ScenarioInvariant::DistinctPrincipals,
            ScenarioInvariant::DeniedObjectPruned,
            ScenarioInvariant::PageTokenScopeBound,
            ScenarioInvariant::StaleIndexRejected,
            ScenarioInvariant::PayloadReadAfterPlanner,
            ScenarioInvariant::CandidateMetricsRecorded,
        ],
    },
    QueryPlannerE2eScenario {
        name: "full text results are intersected with authz candidates before visibility",
        family: QueryFamily::FullText,
        invariants: &[
            ScenarioInvariant::MultipleBoundaries,
            ScenarioInvariant::DistinctPrincipals,
            ScenarioInvariant::DeniedObjectPruned,
            ScenarioInvariant::PageTokenScopeBound,
            ScenarioInvariant::StaleAuthzRejected,
            ScenarioInvariant::CandidateMetricsRecorded,
        ],
    },
    QueryPlannerE2eScenario {
        name: "vector results are intersected with authz candidates before visibility",
        family: QueryFamily::Vector,
        invariants: &[
            ScenarioInvariant::MultipleBoundaries,
            ScenarioInvariant::DistinctPrincipals,
            ScenarioInvariant::DeniedObjectPruned,
            ScenarioInvariant::PageTokenScopeBound,
            ScenarioInvariant::StaleAuthzRejected,
            ScenarioInvariant::CandidateMetricsRecorded,
        ],
    },
    QueryPlannerE2eScenario {
        name: "hybrid results bind both score sources to the unified planner scope",
        family: QueryFamily::Hybrid,
        invariants: &[
            ScenarioInvariant::MultipleBoundaries,
            ScenarioInvariant::DistinctPrincipals,
            ScenarioInvariant::DeniedObjectPruned,
            ScenarioInvariant::PageTokenScopeBound,
            ScenarioInvariant::StaleAuthzRejected,
            ScenarioInvariant::StaleIndexRejected,
            ScenarioInvariant::CandidateMetricsRecorded,
        ],
    },
];

#[test]
fn e2e_scaffold_covers_every_query_family_and_security_invariant() {
    let families = REQUIRED_SCENARIOS
        .iter()
        .map(|scenario| scenario.family)
        .collect::<BTreeSet<_>>();
    assert_eq!(
        families,
        BTreeSet::from([
            QueryFamily::TypedMetadata,
            QueryFamily::TypedJson,
            QueryFamily::FullText,
            QueryFamily::Vector,
            QueryFamily::Hybrid,
        ])
    );

    let invariants = REQUIRED_SCENARIOS
        .iter()
        .flat_map(|scenario| scenario.invariants.iter().copied())
        .collect::<BTreeSet<_>>();
    for required in [
        ScenarioInvariant::MultipleBoundaries,
        ScenarioInvariant::DistinctPrincipals,
        ScenarioInvariant::DeniedObjectPruned,
        ScenarioInvariant::PageTokenScopeBound,
        ScenarioInvariant::StaleAuthzRejected,
        ScenarioInvariant::StaleIndexRejected,
        ScenarioInvariant::PayloadReadAfterPlanner,
        ScenarioInvariant::CandidateMetricsRecorded,
    ] {
        assert!(
            invariants.contains(&required),
            "missing E2E scaffold invariant {required:?}"
        );
    }

    for scenario in REQUIRED_SCENARIOS {
        assert!(
            scenario
                .invariants
                .contains(&ScenarioInvariant::DeniedObjectPruned),
            "{} does not prove unauthorised results are absent",
            scenario.name
        );
        assert!(
            scenario
                .invariants
                .contains(&ScenarioInvariant::CandidateMetricsRecorded),
            "{} does not require candidate pruning metrics",
            scenario.name
        );
    }
}

#[test]
fn public_query_api_exposes_the_fields_needed_by_the_e2e_scaffold() {
    let proto = repo_file("anvil-core/proto/anvil.proto");
    for expected in [
        "message QueryIndexRequest",
        "string page_token",
        "string require_caught_up_to_watch_cursor",
        "uint64 lag_timeout_ms",
        "string boundary_predicates_json",
        "message QueryIndexResponse",
        "uint64 index_generation",
        "uint64 authz_revision",
        "string next_page_token",
        "uint64 source_watch_cursor_high",
        "uint64 index_watch_cursor_applied",
        "bool is_caught_up",
        "uint64 lag_record_count_hint",
    ] {
        assert!(
            proto.contains(expected),
            "missing public API field {expected}"
        );
    }
}

#[test]
fn existing_public_api_tests_are_anchored_to_the_scaffolded_behaviour() {
    let typed_lifecycle = repo_file("anvil/tests/index_tests/typed_lifecycle.rs");
    let query_spec = repo_file("anvil/tests/index_tests/query_spec.rs");
    let vector_hybrid = repo_file("anvil/tests/index_tests/vector_hybrid.rs");

    for expected in [
        "test_live_metadata_query_uses_planner_authz_candidates_and_scoped_page_tokens",
        "tenant-a/denied.json",
        "planner-no-object-reader",
        "first_page.next_page_token",
        "require_caught_up_to_watch_cursor: u64::MAX.to_string()",
    ] {
        assert!(
            typed_lifecycle.contains(expected),
            "missing metadata E2E anchor {expected}"
        );
    }

    for expected in [
        "test_query_index_results_are_filtered_by_zanzibar_object_relationships",
        "test_query_spec_path_filter_intersects_authz_before_results",
        "test_query_spec_inherit_object_filter_uses_derived_userset_grants",
        "authz_scope",
        "auth/denied.json",
    ] {
        assert!(
            query_spec.contains(expected),
            "missing query spec E2E anchor {expected}"
        );
    }

    for expected in [
        "hybrid",
        "vector",
        "QueryIndexRequest",
        "next_page_token",
        "require_caught_up_to_watch_cursor",
    ] {
        assert!(
            vector_hybrid.contains(expected),
            "missing vector/hybrid E2E anchor {expected}"
        );
    }
}
