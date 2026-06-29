use fission::core::op::{AlignItems, BoxShadow, Fill, FlexWrap, JustifyContent};
use fission::prelude::*;
use fission::site::FissionSite;

#[derive(Default, Debug, Clone, PartialEq)]
pub struct DocumentationState;

impl GlobalState for DocumentationState {}

pub fn site_app() -> FissionSite {
    FissionSite::new()
        .light_dark_themes(anvil_light_theme(), anvil_dark_theme(), DesignMode::Light)
        .route_widget::<DocumentationState, _>(
            "/",
            "Anvil",
            Some("A production object store with indexing, search, authorization, watch streams, and PersonalDB witnessing built in.".to_string()),
            HomePage,
        )
        .footer_widget::<DocumentationState, _>(SiteFooter)
}

fn anvil_light_theme() -> Theme {
    let mut tokens = Theme::default().tokens;
    tokens.colors.primary = rgb(30, 92, 220);
    tokens.colors.primary_hover = rgb(19, 66, 174);
    tokens.colors.primary_subtle = rgb(231, 239, 255);
    tokens.colors.secondary = rgb(190, 109, 35);
    tokens.colors.background = rgb(246, 241, 232);
    tokens.colors.surface = rgb(255, 252, 246);
    tokens.colors.surface_raised = rgb(255, 255, 255);
    tokens.colors.surface_sunken = rgb(238, 243, 252);
    tokens.colors.heading = rgb(15, 23, 42);
    tokens.colors.text_primary = rgb(15, 23, 42);
    tokens.colors.text_secondary = rgb(71, 85, 105);
    tokens.colors.text_muted = rgb(100, 116, 139);
    tokens.colors.text_link = rgb(30, 92, 220);
    tokens.colors.border = rgb(210, 219, 233);
    tokens.colors.border_strong = rgb(149, 164, 184);
    tokens.colors.focus_ring = rgb(30, 92, 220);
    Theme::from_tokens(tokens, DesignMode::Light)
}

fn anvil_dark_theme() -> Theme {
    let mut tokens = Theme::dark().tokens;
    tokens.colors.primary = rgb(96, 165, 250);
    tokens.colors.on_primary = rgb(8, 18, 38);
    tokens.colors.primary_hover = rgb(147, 197, 253);
    tokens.colors.primary_subtle = rgb(21, 38, 68);
    tokens.colors.secondary = rgb(251, 191, 36);
    tokens.colors.on_secondary = rgb(27, 19, 4);
    tokens.colors.background = rgb(7, 12, 24);
    tokens.colors.on_background = rgb(226, 232, 240);
    tokens.colors.surface = rgb(13, 20, 34);
    tokens.colors.surface_raised = rgb(17, 27, 46);
    tokens.colors.surface_sunken = rgb(5, 10, 21);
    tokens.colors.on_surface = rgb(226, 232, 240);
    tokens.colors.heading = rgb(248, 250, 252);
    tokens.colors.text_primary = rgb(226, 232, 240);
    tokens.colors.text_secondary = rgb(186, 197, 214);
    tokens.colors.text_muted = rgb(139, 152, 174);
    tokens.colors.text_link = rgb(147, 197, 253);
    tokens.colors.border = rgb(45, 58, 82);
    tokens.colors.border_strong = rgb(74, 94, 130);
    tokens.colors.focus_ring = rgb(251, 191, 36);
    Theme::from_tokens(tokens, DesignMode::Dark)
}

#[derive(Clone)]
struct HomePage;

impl From<HomePage> for Widget {
    fn from(_: HomePage) -> Self {
        let (_ctx, view) = fission::build::current::<DocumentationState>();
        let _tokens = &view.env().theme.tokens;
        Container::new(Column {
            gap: Some(76.0),
            children: vec![
                HeroSection.into(),
                PrincipleSection.into(),
                CapabilitySection.into(),
                FlowSection.into(),
                AudienceSection.into(),
                FinalCta.into(),
            ],
            ..Default::default()
        })
        .bg_fill(Fill::LinearGradient {
            start: (0.0, 0.0),
            end: (1.0, 1.0),
            stops: vec![
                (0.0, rgb(246, 241, 232)),
                (0.55, rgb(238, 243, 252).with_alpha(205)),
                (1.0, rgb(255, 252, 246)),
            ],
        })
        .padding([36.0, 36.0, 42.0, 54.0])
        .into()
    }
}

#[derive(Clone)]
struct HeroSection;

impl From<HeroSection> for Widget {
    fn from(_: HeroSection) -> Self {
        let (_ctx, view) = fission::build::current::<DocumentationState>();
        let tokens = &view.env().theme.tokens;
        Row {
            gap: Some(42.0),
            wrap: FlexWrap::Wrap,
            align_items: AlignItems::Center,
            justify_content: JustifyContent::SpaceBetween,
            children: vec![
                Container::new(Column {
                    gap: Some(22.0),
                    children: vec![
                        Eyebrow::new("Production object storage for product data").into(),
                        Text::new("One storage layer for objects, search, permissions, watches, and local-first data.")
                            .size(62.0)
                            .family(tokens.typography.font_family_serif.clone())
                            .weight(900)
                            .line_height(68.0)
                            .color(rgb(15, 23, 42))
                            .max_width(850.0)
                            .flex_shrink(1.0)
                            .into(),
                        Text::new("Anvil stores bytes, then keeps the derived systems that make those bytes useful attached to the same source of truth: path and metadata indexes, full text search, vector search, relationship authorization, durable watches, source artifacts, model artifacts, and PersonalDB witnessing.")
                            .size(20.0)
                            .line_height(31.0)
                            .color(rgb(71, 85, 105))
                            .max_width(790.0)
                            .flex_shrink(1.0)
                            .into(),
                        Row {
                            gap: Some(12.0),
                            wrap: FlexWrap::Wrap,
                            children: vec![
                                Cta::new("Start learning", "/learn/overview/", true).into(),
                                Cta::new("Run the tutorials", "/tutorials/overview/", false).into(),
                                Cta::new("Deploy Anvil", "/operators/deployment/", false).into(),
                            ],
                            ..Default::default()
                        }
                        .into(),
                    ],
                    ..Default::default()
                })
                .max_width(880.0)
                .flex_grow(1.0)
                .into(),
                SystemMap.into(),
            ],
            ..Default::default()
        }
        .into()
    }
}

#[derive(Clone)]
struct SystemMap;

impl From<SystemMap> for Widget {
    fn from(_: SystemMap) -> Self {
        let (_ctx, view) = fission::build::current::<DocumentationState>();
        let _tokens = &view.env().theme.tokens;
        Container::new(Column {
            gap: Some(16.0),
            children: vec![
                Text::new("A write becomes a platform event")
                    .size(16.0)
                    .weight(900)
                    .color(rgb(15, 23, 42))
                    .into(),
                PipelineLine::new("01", "Commit object", "bytes + metadata + version").into(),
                PipelineLine::new("02", "Maintain views", "path, text, vector, authz, PersonalDB").into(),
                PipelineLine::new("03", "Serve safely", "authorized reads, searches, watches, sync").into(),
                Container::new(Text::new("No sidecar search service guessing what changed. No app-only permission filter trying to hide leaked snippets. No projection job without a cursor.")
                    .size(14.0)
                    .line_height(22.0)
                    .color(rgb(71, 85, 105)))
                    .padding([18.0, 18.0, 16.0, 16.0])
                    .bg_fill(Fill::Solid(rgb(238, 243, 252).with_alpha(210)))
                    .border_radius(20.0)
                    .into(),
            ],
            ..Default::default()
        })
        .width(405.0)
        .bg_fill(Fill::LinearGradient {
            start: (0.0, 0.0),
            end: (1.0, 1.0),
            stops: vec![
                (0.0, rgb(255, 255, 255).with_alpha(246)),
                (1.0, rgb(231, 239, 255).with_alpha(210)),
            ],
        })
        .border(rgb(210, 219, 233), 1.0)
        .border_radius(30.0)
        .padding([24.0, 24.0, 24.0, 24.0])
        .shadow(BoxShadow {
            color: rgb(15, 23, 42).with_alpha(24),
            blur_radius: 28.0,
            offset: (0.0, 18.0),
        })
        .into()
    }
}

#[derive(Clone)]
struct PrincipleSection;

impl From<PrincipleSection> for Widget {
    fn from(_: PrincipleSection) -> Self {
        let (_ctx, view) = fission::build::current::<DocumentationState>();
        let _tokens = &view.env().theme.tokens;
        Container::new(Row {
            gap: Some(44.0),
            wrap: FlexWrap::Wrap,
            align_items: AlignItems::Start,
            children: vec![
                SectionIntro::new(
                    "Why Anvil exists",
                    "Object storage is easy until the object becomes product state.",
                    "The first upload works with any bucket. The hard part arrives when users need filtered timelines, private search, semantic retrieval, live updates, local-first sync, and evidence that every derived view is current. Anvil makes those capabilities part of storage instead of application glue.",
                )
                .into(),
                Container::new(Column {
                    gap: Some(18.0),
                    children: vec![
                        ArgumentLine::new("Search must know permissions", "Counts, snippets, vectors, facets, and suggestions can leak data if authorization is only applied after the query.").into(),
                        ArgumentLine::new("Indexes must prove what they consumed", "Derived state is useful only when it exposes cursors, generations, manifests, lag, and repair findings.").into(),
                        ArgumentLine::new("Local-first sync needs a witness", "SQLite changesets need validation, commit certificates, snapshots, and projections that follow the same authorization model.").into(),
                    ],
                    ..Default::default()
                })
                .max_width(620.0)
                .flex_grow(1.0)
                .into(),
            ],
            ..Default::default()
        })
        .padding([0.0, 0.0, 8.0, 8.0])
        .border(rgb(226, 232, 240), 0.0)
        .into()
    }
}

#[derive(Clone)]
struct CapabilitySection;

impl From<CapabilitySection> for Widget {
    fn from(_: CapabilitySection) -> Self {
        Container::new(Column {
            gap: Some(30.0),
            children: vec![
                SectionIntro::new(
                    "Capabilities",
                    "The feature set is broad because the source facts are shared.",
                    "Objects, indexes, authorization, watch streams, PersonalDB, source artifacts, and model artifacts all refer to the same buckets, keys, versions, checksums, metadata, and durable mutation cursors.",
                )
                .into(),
                Column {
                    gap: Some(0.0),
                    children: vec![
                        CapabilityLine::new("Store", "Buckets, keys, versions, checksums, range reads, multipart uploads, append streams, JSON patching, manifest compare-and-swap.", "/tutorials/objects/").into(),
                        CapabilityLine::new("Find", "Directory indexes, metadata filters, full text search, vector search, hybrid ranking, source indexes, model tensor lookup.", "/tutorials/search/").into(),
                        CapabilityLine::new("Protect", "Token scopes, public access policy, Zanzibar-style tuples, caveats, permission checks, authz watches, fail-closed internal namespaces.", "/tutorials/authorization/").into(),
                        CapabilityLine::new("React", "Bucket metadata watches, prefix watches, index definition watches, partition watches, authz watches, source watches, PersonalDB watches.", "/tutorials/watches/").into(),
                        CapabilityLine::new("Sync", "PersonalDB groups, changesets, catch-up, snapshots, projections, row metadata, projection writeback, witness certificates.", "/tutorials/personaldb/").into(),
                        CapabilityLine::new("Operate", "Index repair, directory repair, authz derived repair, PersonalDB log-chain repair, diagnostics, release smoke tests, package publishing.", "/tutorials/operations/").into(),
                    ],
                    ..Default::default()
                }
                .into(),
            ],
            ..Default::default()
        })
        .into()
    }
}

#[derive(Clone)]
struct FlowSection;

impl From<FlowSection> for Widget {
    fn from(_: FlowSection) -> Self {
        let (_ctx, view) = fission::build::current::<DocumentationState>();
        let _tokens = &view.env().theme.tokens;
        Container::new(Row {
            gap: Some(34.0),
            wrap: FlexWrap::Wrap,
            align_items: AlignItems::Center,
            children: vec![
                SectionIntro::new(
                    "How to evaluate it",
                    "Read the docs as a course, then use the tutorials as operating muscle memory.",
                    "The Learn section teaches concepts from first principles. The tutorial section turns those concepts into operations in Rust, Java, Node.js, and Python. The operator section covers deployment, identity, indexing operations, backup, recovery, and releases.",
                )
                .into(),
                Container::new(Column {
                    gap: Some(14.0),
                    children: vec![
                        StepText::new("1", "Learn", "Understand object storage, keys, indexes, vectors, authorization, watches, and PersonalDB.").into(),
                        StepText::new("2", "Build", "Run tutorials that create buckets, write objects, query indexes, stream watches, and submit PersonalDB changes.").into(),
                        StepText::new("3", "Operate", "Deploy nodes, issue credentials, monitor lag, repair derived data, and publish release artifacts.").into(),
                    ],
                    ..Default::default()
                })
                .max_width(590.0)
                .flex_grow(1.0)
                .into(),
            ],
            ..Default::default()
        })
        .padding([32.0, 32.0, 32.0, 32.0])
        .bg_fill(Fill::Solid(rgb(255, 255, 255).with_alpha(232)))
        .border(rgb(210, 219, 233), 1.0)
        .border_radius(30.0)
        .into()
    }
}

#[derive(Clone)]
struct AudienceSection;

impl From<AudienceSection> for Widget {
    fn from(_: AudienceSection) -> Self {
        Container::new(Column {
            gap: Some(22.0),
            children: vec![
                SectionIntro::new(
                    "Choose your path",
                    "A storage platform has more than one audience.",
                    "Start with the conceptual path if the vocabulary is new. Jump to tutorials when you need a concrete operation. Use operator and reference pages when preparing a deployment or release.",
                )
                .into(),
                Row {
                    gap: Some(22.0),
                    wrap: FlexWrap::Wrap,
                    children: vec![
                        AudienceLink::new("Learn the model", "Concepts from first principles.", "/learn/overview/").into(),
                        AudienceLink::new("Perform operations", "Tutorials in four languages.", "/tutorials/overview/").into(),
                        AudienceLink::new("Run production", "Deployment and operations.", "/operators/deployment/").into(),
                        AudienceLink::new("Check exact settings", "Configuration and errors.", "/reference/configuration/").into(),
                    ],
                    ..Default::default()
                }
                .into(),
            ],
            ..Default::default()
        })
        .into()
    }
}

#[derive(Clone)]
struct FinalCta;

impl From<FinalCta> for Widget {
    fn from(_: FinalCta) -> Self {
        Container::new(Row {
            gap: Some(28.0),
            wrap: FlexWrap::Wrap,
            justify_content: JustifyContent::SpaceBetween,
            align_items: AlignItems::Center,
            children: vec![
                Container::new(Column {
                    gap: Some(12.0),
                    children: vec![
                        Text::new("Adopt Anvil when storage has become product infrastructure.")
                            .size(34.0)
                            .weight(900)
                            .line_height(42.0)
                            .color(Color::WHITE)
                            .max_width(720.0)
                            .into(),
                        Text::new("It gives teams one place to reason about stored bytes, searchable meaning, authorization, live change streams, and local-first database witness state.")
                            .size(17.0)
                            .line_height(27.0)
                            .color(rgba(219, 228, 241, 255))
                            .max_width(760.0)
                            .into(),
                    ],
                    ..Default::default()
                })
                .into(),
                Row {
                    gap: Some(12.0),
                    wrap: FlexWrap::Wrap,
                    children: vec![
                        DarkCta::new("Read the course", "/learn/overview/", true).into(),
                        DarkCta::new("Open tutorials", "/tutorials/overview/", false).into(),
                    ],
                    ..Default::default()
                }
                .into(),
            ],
            ..Default::default()
        })
        .bg_fill(Fill::LinearGradient {
            start: (0.0, 0.0),
            end: (1.0, 1.0),
            stops: vec![(0.0, rgb(10, 20, 38)), (1.0, rgb(22, 44, 82))],
        })
        .border_radius(30.0)
        .padding([34.0, 34.0, 34.0, 34.0])
        .shadow(BoxShadow {
            color: rgba(2, 6, 23, 70),
            blur_radius: 20.0,
            offset: (0.0, 14.0),
        })
        .into()
    }
}

#[derive(Clone)]
struct SectionIntro {
    eyebrow: &'static str,
    title: &'static str,
    body: &'static str,
}

impl SectionIntro {
    fn new(eyebrow: &'static str, title: &'static str, body: &'static str) -> Self {
        Self {
            eyebrow,
            title,
            body,
        }
    }
}

impl From<SectionIntro> for Widget {
    fn from(section: SectionIntro) -> Self {
        let (_ctx, view) = fission::build::current::<DocumentationState>();
        let tokens = &view.env().theme.tokens;
        Container::new(Column {
            gap: Some(13.0),
            children: vec![
                Text::new(section.eyebrow)
                    .size(13.0)
                    .weight(900)
                    .letter_spacing(1.3)
                    .color(rgb(190, 109, 35))
                    .into(),
                Text::new(section.title)
                    .size(40.0)
                    .family(tokens.typography.font_family_serif.clone())
                    .weight(900)
                    .line_height(47.0)
                    .color(rgb(15, 23, 42))
                    .max_width(720.0)
                    .flex_shrink(1.0)
                    .into(),
                Text::new(section.body)
                    .size(18.0)
                    .line_height(29.0)
                    .color(rgb(71, 85, 105))
                    .max_width(770.0)
                    .flex_shrink(1.0)
                    .into(),
            ],
            ..Default::default()
        })
        .max_width(800.0)
        .flex_grow(1.0)
        .into()
    }
}

#[derive(Clone)]
struct PipelineLine {
    number: &'static str,
    title: &'static str,
    body: &'static str,
}

impl PipelineLine {
    fn new(number: &'static str, title: &'static str, body: &'static str) -> Self {
        Self {
            number,
            title,
            body,
        }
    }
}

impl From<PipelineLine> for Widget {
    fn from(line: PipelineLine) -> Self {
        let (_ctx, view) = fission::build::current::<DocumentationState>();
        let _tokens = &view.env().theme.tokens;
        Row {
            gap: Some(12.0),
            align_items: AlignItems::Center,
            children: vec![
                Container::new(
                    Text::new(line.number)
                        .size(12.0)
                        .weight(900)
                        .color(Color::WHITE),
                )
                .bg(rgb(30, 92, 220))
                .border_radius(999.0)
                .padding([10.0, 10.0, 8.0, 8.0])
                .into(),
                Column {
                    gap: Some(3.0),
                    children: vec![
                        Text::new(line.title)
                            .size(16.0)
                            .weight(900)
                            .color(rgb(15, 23, 42))
                            .into(),
                        Text::new(line.body)
                            .size(13.0)
                            .color(rgb(100, 116, 139))
                            .into(),
                    ],
                    ..Default::default()
                }
                .into(),
            ],
            ..Default::default()
        }
        .into()
    }
}

#[derive(Clone)]
struct ArgumentLine {
    title: &'static str,
    body: &'static str,
}

impl ArgumentLine {
    fn new(title: &'static str, body: &'static str) -> Self {
        Self { title, body }
    }
}

impl From<ArgumentLine> for Widget {
    fn from(item: ArgumentLine) -> Self {
        let (_ctx, view) = fission::build::current::<DocumentationState>();
        let _tokens = &view.env().theme.tokens;
        Container::new(Column {
            gap: Some(8.0),
            children: vec![
                Text::new(item.title)
                    .size(22.0)
                    .weight(900)
                    .color(rgb(15, 23, 42))
                    .into(),
                Text::new(item.body)
                    .size(16.0)
                    .line_height(25.0)
                    .color(rgb(71, 85, 105))
                    .max_width(600.0)
                    .into(),
            ],
            ..Default::default()
        })
        .padding([0.0, 0.0, 18.0, 18.0])
        .border(rgb(226, 232, 240), 0.0)
        .into()
    }
}

#[derive(Clone)]
struct CapabilityLine {
    title: &'static str,
    body: &'static str,
    href: &'static str,
}

impl CapabilityLine {
    fn new(title: &'static str, body: &'static str, href: &'static str) -> Self {
        Self { title, body, href }
    }
}

impl From<CapabilityLine> for Widget {
    fn from(item: CapabilityLine) -> Self {
        let (_ctx, view) = fission::build::current::<DocumentationState>();
        let tokens = &view.env().theme.tokens;
        Container::new(Row {
            gap: Some(26.0),
            wrap: FlexWrap::Wrap,
            align_items: AlignItems::Center,
            children: vec![
                Text::new(item.title)
                    .size(28.0)
                    .family(tokens.typography.font_family_serif.clone())
                    .weight(900)
                    .line_height(33.0)
                    .color(rgb(15, 23, 42))
                    .min_width(150.0)
                    .into(),
                Text::new(item.body)
                    .size(16.0)
                    .line_height(25.0)
                    .color(rgb(71, 85, 105))
                    .max_width(760.0)
                    .flex_grow(1.0)
                    .into(),
                InlineLink::new("Tutorial", item.href).into(),
            ],
            ..Default::default()
        })
        .padding([4.0, 4.0, 20.0, 20.0])
        .into()
    }
}

#[derive(Clone)]
struct StepText {
    number: &'static str,
    title: &'static str,
    body: &'static str,
}

impl StepText {
    fn new(number: &'static str, title: &'static str, body: &'static str) -> Self {
        Self {
            number,
            title,
            body,
        }
    }
}

impl From<StepText> for Widget {
    fn from(step: StepText) -> Self {
        let (_ctx, view) = fission::build::current::<DocumentationState>();
        let _tokens = &view.env().theme.tokens;
        Row {
            gap: Some(16.0),
            align_items: AlignItems::Start,
            children: vec![
                Text::new(step.number)
                    .size(34.0)
                    .weight(900)
                    .color(rgb(30, 92, 220))
                    .into(),
                Column {
                    gap: Some(5.0),
                    children: vec![
                        Text::new(step.title)
                            .size(20.0)
                            .weight(900)
                            .color(rgb(15, 23, 42))
                            .into(),
                        Text::new(step.body)
                            .size(15.0)
                            .line_height(24.0)
                            .color(rgb(71, 85, 105))
                            .max_width(500.0)
                            .into(),
                    ],
                    ..Default::default()
                }
                .into(),
            ],
            ..Default::default()
        }
        .into()
    }
}

#[derive(Clone)]
struct AudienceLink {
    title: &'static str,
    body: &'static str,
    href: &'static str,
}

impl AudienceLink {
    fn new(title: &'static str, body: &'static str, href: &'static str) -> Self {
        Self { title, body, href }
    }
}

impl From<AudienceLink> for Widget {
    fn from(item: AudienceLink) -> Self {
        let (_ctx, view) = fission::build::current::<DocumentationState>();
        let _tokens = &view.env().theme.tokens;
        Container::new(Column {
            gap: Some(8.0),
            children: vec![
                Text::new(item.title)
                    .size(18.0)
                    .weight(900)
                    .color(rgb(15, 23, 42))
                    .semantics_identifier(format!("site-route:{}", item.href))
                    .into(),
                Text::new(item.body)
                    .size(14.0)
                    .line_height(21.0)
                    .color(rgb(71, 85, 105))
                    .max_width(230.0)
                    .into(),
            ],
            ..Default::default()
        })
        .padding([0.0, 18.0, 8.0, 8.0])
        .min_width(210.0)
        .into()
    }
}

#[derive(Clone)]
struct Eyebrow {
    label: &'static str,
}

impl Eyebrow {
    fn new(label: &'static str) -> Self {
        Self { label }
    }
}

impl From<Eyebrow> for Widget {
    fn from(eyebrow: Eyebrow) -> Self {
        let (_ctx, view) = fission::build::current::<DocumentationState>();
        let _tokens = &view.env().theme.tokens;
        Text::new(eyebrow.label)
            .size(13.0)
            .weight(900)
            .letter_spacing(1.6)
            .color(rgb(190, 109, 35))
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
        let (_ctx, view) = fission::build::current::<DocumentationState>();
        let _tokens = &view.env().theme.tokens;
        let (background, foreground, border) = if cta.primary {
            (rgb(30, 92, 220), Color::WHITE, rgb(30, 92, 220))
        } else {
            (rgb(255, 255, 255), rgb(15, 23, 42), rgb(210, 219, 233))
        };
        Container::new(
            Text::new(cta.label)
                .size(15.0)
                .weight(900)
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
struct DarkCta {
    label: &'static str,
    href: &'static str,
    primary: bool,
}

impl DarkCta {
    fn new(label: &'static str, href: &'static str, primary: bool) -> Self {
        Self {
            label,
            href,
            primary,
        }
    }
}

impl From<DarkCta> for Widget {
    fn from(cta: DarkCta) -> Self {
        let (background, foreground, border) = if cta.primary {
            (rgb(255, 255, 255), rgb(10, 20, 38), rgb(255, 255, 255))
        } else {
            (
                rgba(255, 255, 255, 0),
                Color::WHITE,
                rgba(148, 163, 184, 150),
            )
        };
        Container::new(
            Text::new(cta.label)
                .size(15.0)
                .weight(900)
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
        let (_ctx, view) = fission::build::current::<DocumentationState>();
        let _tokens = &view.env().theme.tokens;
        Text::new(link.label)
            .size(14.0)
            .weight(900)
            .color(rgb(30, 92, 220))
            .semantics_identifier(format!("site-route:{}", link.href))
            .into()
    }
}

#[derive(Clone)]
struct SiteFooter;

impl From<SiteFooter> for Widget {
    fn from(_: SiteFooter) -> Self {
        let (_ctx, view) = fission::build::current::<DocumentationState>();
        let _tokens = &view.env().theme.tokens;
        Container::new(Row {
            gap: Some(18.0),
            wrap: FlexWrap::Wrap,
            justify_content: JustifyContent::SpaceBetween,
            children: vec![
                Text::new("Anvil storage platform")
                    .size(14.0)
                    .weight(800)
                    .color(rgb(100, 116, 139))
                    .into(),
                Row {
                    gap: Some(14.0),
                    wrap: FlexWrap::Wrap,
                    children: vec![
                        InlineLink::new("Learn", "/learn/overview/").into(),
                        InlineLink::new("Tutorials", "/tutorials/overview/").into(),
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
        .bg(rgb(246, 241, 232))
        .into()
    }
}

fn rgb(r: u8, g: u8, b: u8) -> Color {
    rgba(r, g, b, 255)
}

fn rgba(r: u8, g: u8, b: u8, a: u8) -> Color {
    Color { r, g, b, a }
}
