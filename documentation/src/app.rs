use fission::core::op::{AlignItems, BoxShadow, FlexWrap, JustifyContent};
use fission::prelude::*;
use fission::site::FissionSite;

#[derive(Default, Debug, Clone, PartialEq)]
pub struct DocumentationState;

impl GlobalState for DocumentationState {}

pub fn site_app() -> FissionSite {
    FissionSite::new()
        .light_dark_themes(Theme::default(), Theme::dark(), DesignMode::Light)
        .route_widget::<DocumentationState, _>(
            "/",
            "Anvil",
            Some("A production object store with indexing, search, authorization, watch streams, and PersonalDB witnessing built in.".to_string()),
            HomePage,
        )
        .footer_widget::<DocumentationState, _>(SiteFooter)
}

#[derive(Clone)]
struct HomePage;

impl From<HomePage> for Widget {
    fn from(_: HomePage) -> Self {
        Container::new(Column {
            gap: Some(34.0),
            children: vec![
                HeroSection.into(),
                ProblemSection.into(),
                FeatureGrid.into(),
                WorkflowSection.into(),
                AudienceSection.into(),
                FinalCta.into(),
            ],
            ..Default::default()
        })
        .bg(rgb(247, 244, 237))
        .padding([28.0, 28.0, 30.0, 48.0])
        .into()
    }
}

#[derive(Clone)]
struct HeroSection;

impl From<HeroSection> for Widget {
    fn from(_: HeroSection) -> Self {
        surface(
            Column {
                gap: Some(22.0),
                children: vec![
                    Pill::new("Object storage, indexes, authz, watches, and database witnessing in one system").into(),
                    Text::new("Stop stitching storage, search, permissions, and sync logs together.")
                        .size(58.0)
                        .weight(800)
                        .line_height(1.05)
                        .color(rgb(17, 24, 39))
                        .max_width(980.0)
                        .into(),
                    Text::new("Anvil stores application objects and the systems that make those objects usable: metadata indexes, full text search, vector search, relationship authorization, watch streams, source artifacts, and PersonalDB witness logs. One mutation path feeds every derived view, so teams spend less time reconciling infrastructure and more time building product behavior.")
                        .size(20.0)
                        .line_height(1.5)
                        .color(rgb(55, 65, 81))
                        .max_width(980.0)
                        .into(),
                    Row {
                        gap: Some(12.0),
                        wrap: FlexWrap::Wrap,
                        children: vec![
                            Cta::new("Start learning", "/learn/overview/", true).into(),
                            Cta::new("Developer guides", "/developers/native-api/", false).into(),
                            Cta::new("Operator guide", "/operators/deployment/", false).into(),
                        ],
                        ..Default::default()
                    }
                    .into(),
                    MetricStrip.into(),
                ],
                ..Default::default()
            }
            .into(),
        )
        .into()
    }
}

#[derive(Clone)]
struct MetricStrip;

impl From<MetricStrip> for Widget {
    fn from(_: MetricStrip) -> Self {
        Row {
            gap: Some(12.0),
            wrap: FlexWrap::Wrap,
            children: vec![
                Metric::new("One", "durable mutation stream").into(),
                Metric::new("Five", "native index families").into(),
                Metric::new("Zero", "public access to internal namespaces").into(),
                Metric::new("One", "witness path for PersonalDB").into(),
            ],
            ..Default::default()
        }
        .into()
    }
}

#[derive(Clone)]
struct ProblemSection;

impl From<ProblemSection> for Widget {
    fn from(_: ProblemSection) -> Self {
        Section::new(
            "The problem",
            "Application teams keep rebuilding the same storage control plane.",
            "A product starts with file uploads. Then it needs metadata queries, search, sharing, audit logs, real-time updates, local-first sync, source artifacts, and media indexing. If each feature lands in a separate product, correctness becomes an integration problem. Anvil makes those concerns part of the same storage system.",
            vec![
                ProblemCard::new("Object stores alone do not answer product questions.", "PUT, GET, and LIST are necessary, but users ask for signed contracts, similar images, authorized project files, and database rows that changed since their last sync.").into(),
                ProblemCard::new("Search must respect permissions.", "A search result that leaks the existence of a private document is a security bug. Anvil filters text, vector, metadata, and hybrid results through authorization.").into(),
                ProblemCard::new("Derived systems need a source of truth.", "Indexes, projections, and timelines stay correct only when they are tied to durable mutations, cursors, manifests, and validation proofs.").into(),
            ],
        )
        .into()
    }
}

#[derive(Clone)]
struct FeatureGrid;

impl From<FeatureGrid> for Widget {
    fn from(_: FeatureGrid) -> Self {
        Section::new(
            "What Anvil gives you",
            "A coherent storage platform instead of a pile of adapters.",
            "Each feature is useful alone. The larger value is that they share the same object identity, versioning, authorization, watch, and recovery model.",
            vec![
                FeatureCard::new("Object store", "Buckets, keys, versions, checksums, metadata, range reads, multipart flows, and S3-compatible access for existing tools.", "/learn/object-storage/").into(),
                FeatureCard::new("Metadata and paths", "Predictable key layouts and directory indexes make application timelines, assets, source packs, and control records fast to navigate.", "/learn/keys-paths-and-metadata/").into(),
                FeatureCard::new("Full text search", "Tokenize and rank text from objects, media extraction, or structured row envelopes without scanning buckets at query time.", "/learn/indexes-and-search/").into(),
                FeatureCard::new("Vector search", "Search text, images, audio, and video by semantic similarity using Anvil-owned vector segment formats and Rust-native HNSW indexing.", "/developers/search-and-indexes/").into(),
                FeatureCard::new("Relationship authz", "Model users, groups, workspaces, documents, rows, caveats, and computed usersets with Zanzibar-style tuple semantics.", "/learn/authorization/").into(),
                FeatureCard::new("Watch streams", "Keep indexes, projections, user interfaces, and operational systems current from durable cursors instead of broad rescans.", "/learn/watches-and-derived-data/").into(),
                FeatureCard::new("PersonalDB witness", "Accept SQLite changesets, verify row effects, sign commit certificates, maintain snapshots, and build authorized projections.", "/learn/personaldb/").into(),
                FeatureCard::new("Source artifacts", "Store source packs, build outputs, model manifests, screenshots, logs, and generated artifacts with searchable metadata and authorization.", "/developers/source-and-model-artifacts/").into(),
            ],
        )
        .into()
    }
}

#[derive(Clone)]
struct WorkflowSection;

impl From<WorkflowSection> for Widget {
    fn from(_: WorkflowSection) -> Self {
        Section::new(
            "How the pieces work together",
            "One object write becomes storage, indexes, watches, and authorization-aware results.",
            "Anvil does not ask every application to coordinate search, authz, and derived state by hand. The write path records the durable object and emits the facts that downstream systems consume.",
            vec![
                StepCard::new("01", "Write", "A client writes an object or PersonalDB changeset with metadata, preconditions, and an idempotency key.").into(),
                StepCard::new("02", "Commit", "Anvil validates identity, reserved namespaces, policy, object shape, hashes, and durable journal records before acknowledging.").into(),
                StepCard::new("03", "Derive", "Directory, metadata, full text, vector, authz, source, and PersonalDB projection systems consume watch events and checkpoint cursors.").into(),
                StepCard::new("04", "Serve", "Reads, listings, search, and database sync return versioned, authorized results tied back to the source mutation stream.").into(),
            ],
        )
        .into()
    }
}

#[derive(Clone)]
struct AudienceSection;

impl From<AudienceSection> for Widget {
    fn from(_: AudienceSection) -> Self {
        Section::new(
            "Choose your path",
            "The documentation teaches concepts first, then shows how to build and operate Anvil.",
            "Start with the Learn section if any concept is unfamiliar. The developer and operator sections assume you understand the model and want to apply it.",
            vec![
                FeatureCard::new("Learn", "A progressive introduction for readers who are new to object stores, indexing, vector search, Zanzibar-style authorization, watches, or PersonalDB.", "/learn/overview/").into(),
                FeatureCard::new("Developers", "Build with native gRPC APIs, S3-compatible clients, object metadata, search, PersonalDB, and source artifact workflows.", "/developers/native-api/").into(),
                FeatureCard::new("Operators", "Deploy nodes, manage credentials and relationship authorization, monitor derived indexes, run backups, and publish releases.", "/operators/deployment/").into(),
            ],
        )
        .into()
    }
}

#[derive(Clone)]
struct FinalCta;

impl From<FinalCta> for Widget {
    fn from(_: FinalCta) -> Self {
        Container::new(
            Column {
                gap: Some(16.0),
                children: vec![
                    Text::new("Adopt Anvil when storage has become product infrastructure.")
                        .size(34.0)
                        .weight(800)
                        .line_height(1.15)
                        .color(Color::WHITE)
                        .into(),
                    Text::new("If your team is already coordinating objects, search, permissions, derived views, local database sync, and artifact storage, Anvil gives those concerns one coherent home.")
                        .size(18.0)
                        .line_height(1.45)
                        .color(rgba(229, 231, 235, 255))
                        .max_width(840.0)
                        .into(),
                    Row {
                        gap: Some(12.0),
                        wrap: FlexWrap::Wrap,
                        children: vec![
                            Cta::new("Read the learning path", "/learn/overview/", true).into(),
                            Cta::new("Configure a deployment", "/operators/deployment/", false).into(),
                        ],
                        ..Default::default()
                    }
                    .into(),
                ],
                ..Default::default()
            },
        )
        .bg(rgb(17, 24, 39))
        .border_radius(28.0)
        .padding([34.0, 34.0, 34.0, 34.0])
        .shadow(BoxShadow {
            color: rgba(0, 0, 0, 34),
            blur_radius: 18.0,
            offset: (0.0, 12.0),
        })
        .into()
    }
}

#[derive(Clone)]
struct SiteFooter;

impl From<SiteFooter> for Widget {
    fn from(_: SiteFooter) -> Self {
        Container::new(Row {
            gap: Some(18.0),
            wrap: FlexWrap::Wrap,
            justify_content: JustifyContent::SpaceBetween,
            children: vec![
                Text::new("Anvil storage platform")
                    .size(14.0)
                    .weight(700)
                    .color(rgb(75, 85, 99))
                    .into(),
                Row {
                    gap: Some(14.0),
                    wrap: FlexWrap::Wrap,
                    children: vec![
                        InlineLink::new("Learn", "/learn/overview/").into(),
                        InlineLink::new("Developers", "/developers/native-api/").into(),
                        InlineLink::new("Operators", "/operators/deployment/").into(),
                        InlineLink::new("Reference", "/reference/configuration/").into(),
                    ],
                    ..Default::default()
                }
                .into(),
            ],
            ..Default::default()
        })
        .padding([28.0, 28.0, 18.0, 18.0])
        .into()
    }
}

#[derive(Clone)]
struct Section {
    eyebrow: &'static str,
    title: &'static str,
    body: &'static str,
    cards: Vec<Widget>,
}

impl Section {
    fn new(
        eyebrow: &'static str,
        title: &'static str,
        body: &'static str,
        cards: Vec<Widget>,
    ) -> Self {
        Self {
            eyebrow,
            title,
            body,
            cards,
        }
    }
}

impl From<Section> for Widget {
    fn from(section: Section) -> Self {
        Container::new(Column {
            gap: Some(20.0),
            children: vec![
                Pill::new(section.eyebrow).into(),
                Text::new(section.title)
                    .size(36.0)
                    .weight(800)
                    .line_height(1.12)
                    .color(rgb(17, 24, 39))
                    .max_width(820.0)
                    .into(),
                Text::new(section.body)
                    .size(18.0)
                    .line_height(1.55)
                    .color(rgb(75, 85, 99))
                    .max_width(900.0)
                    .into(),
                Row {
                    gap: Some(14.0),
                    wrap: FlexWrap::Wrap,
                    align_items: AlignItems::Stretch,
                    children: section.cards,
                    ..Default::default()
                }
                .into(),
            ],
            ..Default::default()
        })
        .padding([4.0, 4.0, 14.0, 14.0])
        .into()
    }
}

#[derive(Clone)]
struct FeatureCard {
    title: &'static str,
    body: &'static str,
    href: &'static str,
}

impl FeatureCard {
    fn new(title: &'static str, body: &'static str, href: &'static str) -> Self {
        Self { title, body, href }
    }
}

impl From<FeatureCard> for Widget {
    fn from(card: FeatureCard) -> Self {
        card_shell(
            Column {
                gap: Some(12.0),
                children: vec![
                    Text::new(card.title)
                        .size(21.0)
                        .weight(800)
                        .color(rgb(17, 24, 39))
                        .into(),
                    Text::new(card.body)
                        .size(15.0)
                        .line_height(1.45)
                        .color(rgb(75, 85, 99))
                        .max_width(320.0)
                        .into(),
                    InlineLink::new("Read guide ->", card.href).into(),
                ],
                ..Default::default()
            }
            .into(),
        )
        .into()
    }
}

#[derive(Clone)]
struct ProblemCard {
    title: &'static str,
    body: &'static str,
}

impl ProblemCard {
    fn new(title: &'static str, body: &'static str) -> Self {
        Self { title, body }
    }
}

impl From<ProblemCard> for Widget {
    fn from(card: ProblemCard) -> Self {
        card_shell(
            Column {
                gap: Some(10.0),
                children: vec![
                    Text::new(card.title)
                        .size(20.0)
                        .weight(800)
                        .color(rgb(17, 24, 39))
                        .into(),
                    Text::new(card.body)
                        .size(15.0)
                        .line_height(1.45)
                        .color(rgb(75, 85, 99))
                        .max_width(330.0)
                        .into(),
                ],
                ..Default::default()
            }
            .into(),
        )
        .into()
    }
}

#[derive(Clone)]
struct StepCard {
    step: &'static str,
    title: &'static str,
    body: &'static str,
}

impl StepCard {
    fn new(step: &'static str, title: &'static str, body: &'static str) -> Self {
        Self { step, title, body }
    }
}

impl From<StepCard> for Widget {
    fn from(card: StepCard) -> Self {
        card_shell(
            Column {
                gap: Some(10.0),
                children: vec![
                    Text::new(card.step)
                        .size(13.0)
                        .weight(900)
                        .letter_spacing(1.6)
                        .color(rgb(31, 111, 235))
                        .into(),
                    Text::new(card.title)
                        .size(20.0)
                        .weight(800)
                        .color(rgb(17, 24, 39))
                        .into(),
                    Text::new(card.body)
                        .size(15.0)
                        .line_height(1.45)
                        .color(rgb(75, 85, 99))
                        .max_width(310.0)
                        .into(),
                ],
                ..Default::default()
            }
            .into(),
        )
        .into()
    }
}

#[derive(Clone)]
struct Metric {
    value: &'static str,
    label: &'static str,
}

impl Metric {
    fn new(value: &'static str, label: &'static str) -> Self {
        Self { value, label }
    }
}

impl From<Metric> for Widget {
    fn from(metric: Metric) -> Self {
        Container::new(Column {
            gap: Some(2.0),
            children: vec![
                Text::new(metric.value)
                    .size(28.0)
                    .weight(900)
                    .color(rgb(31, 111, 235))
                    .into(),
                Text::new(metric.label)
                    .size(13.0)
                    .weight(700)
                    .color(rgb(75, 85, 99))
                    .max_width(160.0)
                    .into(),
            ],
            ..Default::default()
        })
        .bg(rgb(248, 250, 252))
        .border(rgb(217, 223, 232), 1.0)
        .border_radius(18.0)
        .padding([18.0, 18.0, 14.0, 14.0])
        .into()
    }
}

#[derive(Clone)]
struct Pill {
    label: &'static str,
}

impl Pill {
    fn new(label: &'static str) -> Self {
        Self { label }
    }
}

impl From<Pill> for Widget {
    fn from(pill: Pill) -> Self {
        Container::new(
            Text::new(pill.label)
                .size(13.0)
                .weight(800)
                .letter_spacing(0.6)
                .color(rgb(15, 118, 110)),
        )
        .bg(rgb(228, 246, 241))
        .border(rgb(153, 216, 201), 1.0)
        .border_radius(999.0)
        .padding([14.0, 14.0, 8.0, 8.0])
        .into()
    }
}

#[derive(Clone)]
struct Cta {
    label: &'static str,
    href: &'static str,
    primary: bool,
}

impl Cta {
    fn new(label: &'static str, href: &'static str, primary: bool) -> Self {
        Self {
            label,
            href,
            primary,
        }
    }
}

impl From<Cta> for Widget {
    fn from(cta: Cta) -> Self {
        let (background, foreground, border) = if cta.primary {
            (rgb(31, 111, 235), Color::WHITE, rgb(31, 111, 235))
        } else {
            (rgb(255, 255, 255), rgb(17, 24, 39), rgb(209, 216, 226))
        };
        Container::new(
            Text::new(cta.label)
                .size(15.0)
                .weight(800)
                .color(foreground)
                .semantics_identifier(format!("site-route:{}", cta.href)),
        )
        .bg(background)
        .border(border, 1.0)
        .border_radius(999.0)
        .padding([18.0, 18.0, 11.0, 11.0])
        .into()
    }
}

#[derive(Clone)]
struct InlineLink {
    label: &'static str,
    href: &'static str,
}

impl InlineLink {
    fn new(label: &'static str, href: &'static str) -> Self {
        Self { label, href }
    }
}

impl From<InlineLink> for Widget {
    fn from(link: InlineLink) -> Self {
        Text::new(link.label)
            .size(14.0)
            .weight(800)
            .color(rgb(31, 111, 235))
            .semantics_identifier(format!("site-route:{}", link.href))
            .into()
    }
}

fn surface(child: Widget) -> Container {
    Container::new(child)
        .bg(rgb(255, 255, 255))
        .border(rgb(226, 232, 240), 1.0)
        .border_radius(32.0)
        .padding([34.0, 34.0, 34.0, 34.0])
        .shadow(BoxShadow {
            color: rgba(15, 23, 42, 22),
            blur_radius: 24.0,
            offset: (0.0, 18.0),
        })
}

fn card_shell(child: Widget) -> Container {
    Container::new(child)
        .bg(rgb(255, 255, 255))
        .border(rgb(226, 232, 240), 1.0)
        .border_radius(22.0)
        .padding([22.0, 22.0, 20.0, 20.0])
        .min_width(260.0)
        .max_width(360.0)
        .shadow(BoxShadow {
            color: rgba(15, 23, 42, 16),
            blur_radius: 12.0,
            offset: (0.0, 8.0),
        })
}

fn rgb(r: u8, g: u8, b: u8) -> Color {
    rgba(r, g, b, 255)
}

fn rgba(r: u8, g: u8, b: u8, a: u8) -> Color {
    Color { r, g, b, a }
}
