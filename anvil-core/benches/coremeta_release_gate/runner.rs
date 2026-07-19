use std::hint::black_box;
use std::time::Instant;

use anvil_core::core_store::{
    CF_INLINE_PAYLOADS, CoreMetaBatchOp, CoreMetaBatchOpKind, CoreMetaStore,
    TABLE_INLINE_PAYLOAD_ROW,
};
use anyhow::{Result, bail};
use rocksdb::perf::{PerfContext, PerfStatsLevel, set_perf_stats};

use crate::config::{GateManifest, ProfileSpec};
use crate::dataset::{
    Dataset, LARGE_PREFIX, MUTATION_PREFIX, SMALL_PREFIX, encoded_payload, prefix_key, row_key,
};
use crate::report::{GateEvidence, ScenarioEvidence, WorkCounters};

pub struct RunResult {
    pub scenarios: Vec<ScenarioEvidence>,
    pub gates: Vec<GateEvidence>,
}

pub fn run(manifest: &GateManifest, profile: &ProfileSpec) -> Result<RunResult> {
    let dataset = Dataset::create(manifest, profile)?;
    set_perf_stats(PerfStatsLevel::EnableCount);

    let point_small = measure_point_get(
        &dataset.store,
        SMALL_PREFIX,
        profile.small_rows,
        profile,
        "point_get_small",
    )?;
    let point_large = measure_point_get(
        &dataset.store,
        LARGE_PREFIX,
        profile.large_rows,
        profile,
        "point_get_large",
    )?;
    let page_early_large = measure_prefix_page(
        &dataset.store,
        LARGE_PREFIX,
        profile.large_rows,
        None,
        profile.page_size,
        profile,
        "prefix_page_early_large",
    )?;
    let page_deep_small = measure_prefix_page(
        &dataset.store,
        SMALL_PREFIX,
        profile.small_rows,
        Some(profile.small_rows - profile.page_size as u64 - 1),
        profile.page_size,
        profile,
        "prefix_page_deep_small",
    )?;
    let page_deep_large = measure_prefix_page(
        &dataset.store,
        LARGE_PREFIX,
        profile.large_rows,
        Some(profile.large_rows - profile.page_size as u64 - 1),
        profile.page_size,
        profile,
        "prefix_page_deep_large",
    )?;
    let page_scaled_large = measure_prefix_page(
        &dataset.store,
        LARGE_PREFIX,
        profile.large_rows,
        Some(profile.large_rows - profile.scaling_page_size as u64 - 1),
        profile.scaling_page_size,
        profile,
        "bounded_list_scaled_page_large",
    )?;
    let durable_single = measure_durable_single_row(
        &dataset.store,
        &manifest.seed,
        profile,
        "durable_single_row",
    )?;
    let transactional_head = measure_transactional_head_batch(
        &dataset.store,
        &manifest.seed,
        profile,
        "transactional_head_read_and_batch",
    )?;

    set_perf_stats(PerfStatsLevel::Disable);

    let scenarios = vec![
        point_small,
        point_large,
        page_early_large,
        page_deep_small,
        page_deep_large,
        page_scaled_large,
        durable_single,
        transactional_head,
    ];
    let gates = evaluate_gates(&scenarios, profile)?;
    Ok(RunResult { scenarios, gates })
}

fn measure_point_get(
    store: &CoreMetaStore,
    prefix: &str,
    dataset_rows: u64,
    profile: &ProfileSpec,
    name: &str,
) -> Result<ScenarioEvidence> {
    let operation_count = profile.warmup_operations + profile.point_samples;
    let keys = (0..operation_count)
        .map(|index| {
            let ordinal =
                ((index as u64).saturating_mul(7_919).saturating_add(104_729)) % dataset_rows;
            row_key(prefix, ordinal)
        })
        .collect::<Result<Vec<_>>>()?;

    measure(
        name,
        dataset_rows,
        1,
        profile.warmup_operations,
        profile.point_samples,
        |operation_index| {
            let value = store
                .get_inline_payload(&keys[operation_index])?
                .ok_or_else(|| anyhow::anyhow!("point benchmark row is missing"))?;
            black_box(value.len());
            Ok(1)
        },
    )
}

#[allow(clippy::too_many_arguments)]
fn measure_prefix_page(
    store: &CoreMetaStore,
    prefix: &str,
    dataset_rows: u64,
    after_ordinal: Option<u64>,
    page_size: usize,
    profile: &ProfileSpec,
    name: &str,
) -> Result<ScenarioEvidence> {
    let tuple_prefix = prefix_key(prefix)?;
    let after_key = after_ordinal
        .map(|ordinal| row_key(prefix, ordinal))
        .transpose()?;

    measure(
        name,
        dataset_rows,
        page_size,
        profile.warmup_operations,
        profile.page_samples,
        |_| {
            let rows = store.scan_prefix_page(
                CF_INLINE_PAYLOADS,
                TABLE_INLINE_PAYLOAD_ROW,
                &tuple_prefix,
                after_key.as_deref(),
                page_size,
            )?;
            black_box(rows.iter().map(|row| row.payload.len()).sum::<usize>());
            Ok(rows.len())
        },
    )
}

fn measure_durable_single_row(
    store: &CoreMetaStore,
    seed: &str,
    profile: &ProfileSpec,
    name: &str,
) -> Result<ScenarioEvidence> {
    let operation_count = profile.warmup_operations + profile.mutation_samples;
    let key = row_key(MUTATION_PREFIX, 0)?;
    let payloads = (0..operation_count)
        .map(|index| {
            encoded_payload(
                seed,
                "durable-single",
                index as u64,
                profile.payload_bytes,
                index as u64 + 1,
            )
        })
        .collect::<Result<Vec<_>>>()?;

    measure(
        name,
        operation_count as u64,
        1,
        profile.warmup_operations,
        profile.mutation_samples,
        |operation_index| {
            let operation = CoreMetaBatchOp {
                cf: CF_INLINE_PAYLOADS,
                table_id: TABLE_INLINE_PAYLOAD_ROW,
                tuple_key: &key,
                common: None,
                kind: CoreMetaBatchOpKind::Put(&payloads[operation_index]),
            };
            store.write_batch(&[operation])?;
            Ok(1)
        },
    )
}

fn measure_transactional_head_batch(
    store: &CoreMetaStore,
    seed: &str,
    profile: &ProfileSpec,
    name: &str,
) -> Result<ScenarioEvidence> {
    let operation_count = profile.warmup_operations + profile.mutation_samples;
    let head_key = row_key(MUTATION_PREFIX, 1)?;
    let version_keys = (0..operation_count)
        .map(|index| row_key(MUTATION_PREFIX, index as u64 + 2))
        .collect::<Result<Vec<_>>>()?;
    let head_payloads = (0..=operation_count)
        .map(|index| {
            encoded_payload(
                seed,
                "transactional-head",
                index as u64,
                profile.payload_bytes,
                index as u64 + 1,
            )
        })
        .collect::<Result<Vec<_>>>()?;
    write_one(store, &head_key, &head_payloads[0])?;

    let evidence = measure(
        name,
        operation_count as u64,
        2,
        profile.warmup_operations,
        profile.mutation_samples,
        |operation_index| {
            let current = store
                .get(CF_INLINE_PAYLOADS, TABLE_INLINE_PAYLOAD_ROW, &head_key)?
                .ok_or_else(|| anyhow::anyhow!("transactional head is missing"))?;
            if current != head_payloads[operation_index] {
                bail!("transactional head did not retain the preceding generation");
            }

            let version_operation = CoreMetaBatchOp {
                cf: CF_INLINE_PAYLOADS,
                table_id: TABLE_INLINE_PAYLOAD_ROW,
                tuple_key: &version_keys[operation_index],
                common: None,
                kind: CoreMetaBatchOpKind::Put(&head_payloads[operation_index + 1]),
            };
            let head_operation = CoreMetaBatchOp {
                cf: CF_INLINE_PAYLOADS,
                table_id: TABLE_INLINE_PAYLOAD_ROW,
                tuple_key: &head_key,
                common: None,
                kind: CoreMetaBatchOpKind::Put(&head_payloads[operation_index + 1]),
            };
            store.write_batch(&[version_operation, head_operation])?;
            Ok(2)
        },
    )?;

    let final_head = store
        .get(CF_INLINE_PAYLOADS, TABLE_INLINE_PAYLOAD_ROW, &head_key)?
        .ok_or_else(|| anyhow::anyhow!("transactional head disappeared"))?;
    if final_head != head_payloads[operation_count] {
        bail!("transactional head does not contain the final generation");
    }
    Ok(evidence)
}

fn write_one(store: &CoreMetaStore, key: &[u8], payload: &[u8]) -> Result<()> {
    store.write_batch(&[CoreMetaBatchOp {
        cf: CF_INLINE_PAYLOADS,
        table_id: TABLE_INLINE_PAYLOAD_ROW,
        tuple_key: key,
        common: None,
        kind: CoreMetaBatchOpKind::Put(payload),
    }])
}

fn measure(
    name: &str,
    dataset_rows: u64,
    expected_items: usize,
    warmup_operations: usize,
    samples: usize,
    mut operation: impl FnMut(usize) -> Result<usize>,
) -> Result<ScenarioEvidence> {
    for operation_index in 0..warmup_operations {
        let returned = operation(operation_index)?;
        if returned != expected_items {
            bail!("{name} warmup returned {returned} items, expected {expected_items}");
        }
    }

    let mut perf_context = PerfContext::default();
    perf_context.reset();
    let mut latency_ns = Vec::with_capacity(samples);
    let mut returned_items = 0_u64;
    let mut item_count_mismatches = 0_u64;
    for sample in 0..samples {
        let operation_index = warmup_operations + sample;
        let started_at = Instant::now();
        let returned = operation(operation_index)?;
        latency_ns.push(started_at.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64);
        returned_items = returned_items.saturating_add(returned as u64);
        if returned != expected_items {
            item_count_mismatches = item_count_mismatches.saturating_add(1);
        }
    }
    let work = WorkCounters::capture(&perf_context);

    Ok(ScenarioEvidence::from_samples(
        name,
        dataset_rows,
        expected_items,
        returned_items,
        item_count_mismatches,
        latency_ns,
        work,
    ))
}

fn evaluate_gates(
    scenarios: &[ScenarioEvidence],
    profile: &ProfileSpec,
) -> Result<Vec<GateEvidence>> {
    let point_small = scenario(scenarios, "point_get_small")?;
    let point_large = scenario(scenarios, "point_get_large")?;
    let page_early = scenario(scenarios, "prefix_page_early_large")?;
    let page_deep_small = scenario(scenarios, "prefix_page_deep_small")?;
    let page_deep_large = scenario(scenarios, "prefix_page_deep_large")?;
    let page_scaled = scenario(scenarios, "bounded_list_scaled_page_large")?;
    let durable_single = scenario(scenarios, "durable_single_row")?;
    let transactional = scenario(scenarios, "transactional_head_read_and_batch")?;
    let thresholds = &profile.thresholds;
    let mut gates = Vec::new();

    for evidence in scenarios {
        gates.push(exact_gate(
            &format!("{}_bounded_result", evidence.name),
            "correctness",
            "items_per_operation",
            evidence.item_count_mismatches as f64,
            0.0,
            "operations",
            "Every operation must return or mutate exactly the declared bounded item count.",
        ));
    }

    let read_counter_total = [
        point_small,
        point_large,
        page_early,
        page_deep_small,
        page_deep_large,
        page_scaled,
    ]
    .iter()
    .map(|evidence| evidence.work.logical_work())
    .sum::<u64>() as f64;
    gates.push(min_gate(
        "rocksdb_work_counters_present",
        "correctness",
        "logical_work",
        read_counter_total,
        1.0,
        "count",
        "A complexity gate cannot pass when RocksDB work counters are absent.",
    ));

    for point in [point_small, point_large] {
        gates.push(max_gate(
            &format!("{}_p95", point.name),
            "latency",
            "p95_ms",
            point.p95_ms,
            thresholds.point_get_p95_ms,
            thresholds.point_get_p95_ms,
            "ms",
            "Point-get p95 uses a deliberately conservative hard ceiling.",
        ));
    }

    let page_latency_limit = thresholds
        .page_p95_floor_ms
        .max(point_large.p95_ms * thresholds.page_p95_point_multiplier);
    for page in [page_early, page_deep_small, page_deep_large, page_scaled] {
        gates.push(max_gate(
            &format!("{}_p95", page.name),
            "latency",
            "p95_ms",
            page.p95_ms,
            thresholds.page_p95_floor_ms,
            page_latency_limit,
            "ms",
            "Page latency is calibrated to point-read p95 with a fixed lower ceiling.",
        ));
    }

    gates.push(max_gate(
        "durable_single_row_p95",
        "latency",
        "p95_ms",
        durable_single.p95_ms,
        thresholds.durable_single_p95_ms,
        thresholds.durable_single_p95_ms,
        "ms",
        "Single-row durable write is the local fsync calibration primitive.",
    ));
    let transactional_latency_limit = thresholds
        .transactional_head_p95_floor_ms
        .max(durable_single.p95_ms * thresholds.transactional_head_single_multiplier);
    gates.push(max_gate(
        "transactional_head_read_and_batch_p95",
        "latency",
        "p95_ms",
        transactional.p95_ms,
        thresholds.transactional_head_p95_floor_ms,
        transactional_latency_limit,
        "ms",
        "Head read plus one two-row atomic batch is calibrated to local durable-write p95.",
    ));

    gates.push(ratio_gate(
        "point_get_work_is_table_size_independent",
        "complexity",
        "logical_work_per_operation_ratio",
        point_large.logical_work_per_operation,
        point_small.logical_work_per_operation,
        thresholds.point_work_growth_ratio,
        "ratio",
        "Increasing rows by at least 8x must not produce table-sized point-read work.",
    ));
    gates.push(ratio_gate(
        "deep_page_work_is_table_size_independent",
        "complexity",
        "logical_work_per_operation_ratio",
        page_deep_large.logical_work_per_operation,
        page_deep_small.logical_work_per_operation,
        thresholds.deep_page_work_growth_ratio,
        "ratio",
        "A deep cursor must seek; it must not walk rows preceding the cursor.",
    ));
    gates.push(ratio_gate(
        "deep_page_work_is_comparable_to_early_page",
        "complexity",
        "logical_work_per_operation_ratio",
        page_deep_large.logical_work_per_operation,
        page_early.logical_work_per_operation,
        thresholds.deep_to_early_work_ratio,
        "ratio",
        "Deep and early pages of equal size must perform comparable bounded work.",
    ));
    let page_size_ratio = profile.scaling_page_size as f64 / profile.page_size as f64;
    gates.push(ratio_gate(
        "bounded_list_work_scales_with_page_size",
        "complexity",
        "logical_work_per_operation_ratio",
        page_scaled.logical_work_per_operation,
        page_deep_large.logical_work_per_operation,
        page_size_ratio * thresholds.page_size_work_ratio_multiplier,
        "ratio",
        "Increasing only page size may increase work proportionally, not with table rows.",
    ));

    for (page, page_size) in [
        (page_early, profile.page_size),
        (page_deep_small, profile.page_size),
        (page_deep_large, profile.page_size),
        (page_scaled, profile.scaling_page_size),
    ] {
        let work_limit =
            page_size as f64 * thresholds.page_work_per_item + thresholds.page_fixed_work;
        gates.push(max_gate(
            &format!("{}_bounded_work", page.name),
            "complexity",
            "logical_work_per_operation",
            page.logical_work_per_operation,
            work_limit,
            work_limit,
            "count/op",
            "Absolute iterator work is capped by page size plus fixed seek overhead.",
        ));
    }

    Ok(gates)
}

fn scenario<'a>(scenarios: &'a [ScenarioEvidence], name: &str) -> Result<&'a ScenarioEvidence> {
    scenarios
        .iter()
        .find(|scenario| scenario.name == name)
        .ok_or_else(|| anyhow::anyhow!("missing benchmark scenario {name}"))
}

#[allow(clippy::too_many_arguments)]
fn max_gate(
    name: &str,
    category: &str,
    metric: &str,
    observed: f64,
    configured_threshold: f64,
    effective_threshold: f64,
    unit: &str,
    detail: &str,
) -> GateEvidence {
    GateEvidence {
        name: name.to_string(),
        category: category.to_string(),
        metric: metric.to_string(),
        expectation: "<=".to_string(),
        observed: observed.is_finite().then_some(observed),
        configured_threshold,
        effective_threshold,
        unit: unit.to_string(),
        pass: observed.is_finite() && observed <= effective_threshold,
        detail: detail.to_string(),
    }
}

fn exact_gate(
    name: &str,
    category: &str,
    metric: &str,
    observed: f64,
    expected: f64,
    unit: &str,
    detail: &str,
) -> GateEvidence {
    GateEvidence {
        name: name.to_string(),
        category: category.to_string(),
        metric: metric.to_string(),
        expectation: "==".to_string(),
        observed: observed.is_finite().then_some(observed),
        configured_threshold: expected,
        effective_threshold: expected,
        unit: unit.to_string(),
        pass: observed.is_finite() && (observed - expected).abs() < f64::EPSILON,
        detail: detail.to_string(),
    }
}

fn min_gate(
    name: &str,
    category: &str,
    metric: &str,
    observed: f64,
    threshold: f64,
    unit: &str,
    detail: &str,
) -> GateEvidence {
    GateEvidence {
        name: name.to_string(),
        category: category.to_string(),
        metric: metric.to_string(),
        expectation: ">=".to_string(),
        observed: observed.is_finite().then_some(observed),
        configured_threshold: threshold,
        effective_threshold: threshold,
        unit: unit.to_string(),
        pass: observed.is_finite() && observed >= threshold,
        detail: detail.to_string(),
    }
}

fn ratio_gate(
    name: &str,
    category: &str,
    metric: &str,
    numerator: f64,
    denominator: f64,
    threshold: f64,
    unit: &str,
    detail: &str,
) -> GateEvidence {
    let observed = (denominator > 0.0).then(|| numerator / denominator);
    GateEvidence {
        name: name.to_string(),
        category: category.to_string(),
        metric: metric.to_string(),
        expectation: "<=".to_string(),
        observed: observed.filter(|value| value.is_finite()),
        configured_threshold: threshold,
        effective_threshold: threshold,
        unit: unit.to_string(),
        pass: observed.is_some_and(|value| value.is_finite() && value <= threshold),
        detail: detail.to_string(),
    }
}
