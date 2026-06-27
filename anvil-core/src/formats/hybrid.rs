#[derive(Debug, Clone, Copy, PartialEq)]
pub struct HybridScoreInput {
    pub normalized_text_score: Option<f32>,
    pub normalized_vector_score: Option<f32>,
    pub freshness_score: Option<f32>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct HybridScoringRecipe {
    pub text_weight: f32,
    pub vector_weight: f32,
    pub freshness_weight: f32,
}

impl HybridScoringRecipe {
    pub const DEFAULT: Self = Self {
        text_weight: 0.55,
        vector_weight: 0.35,
        freshness_weight: 0.10,
    };

    pub fn for_input(input: HybridScoreInput) -> Self {
        match (
            input.normalized_text_score.is_some(),
            input.normalized_vector_score.is_some(),
            input.freshness_score.is_some(),
        ) {
            (true, true, _) => Self::DEFAULT,
            (true, false, false) | (true, false, true) => Self {
                text_weight: 1.0,
                vector_weight: 0.0,
                freshness_weight: 0.0,
            },
            (false, true, false) | (false, true, true) => Self {
                text_weight: 0.0,
                vector_weight: 1.0,
                freshness_weight: 0.0,
            },
            (false, false, true) => Self {
                text_weight: 0.0,
                vector_weight: 0.0,
                freshness_weight: 1.0,
            },
            (false, false, false) => Self {
                text_weight: 0.0,
                vector_weight: 0.0,
                freshness_weight: 0.0,
            },
        }
    }

    pub fn label(self) -> String {
        format!(
            "score = {:.2} * normalized_text_score + {:.2} * normalized_vector_score + {:.2} * freshness_score",
            self.text_weight, self.vector_weight, self.freshness_weight
        )
    }
}

pub fn hybrid_score(input: HybridScoreInput) -> (f32, HybridScoringRecipe) {
    let recipe = HybridScoringRecipe::for_input(input);
    let score = input.normalized_text_score.unwrap_or(0.0).clamp(0.0, 1.0) * recipe.text_weight
        + input.normalized_vector_score.unwrap_or(0.0).clamp(0.0, 1.0) * recipe.vector_weight
        + input.freshness_score.unwrap_or(0.0).clamp(0.0, 1.0) * recipe.freshness_weight;
    (score, recipe)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hybrid_score_uses_default_text_vector_freshness_recipe() {
        let (score, recipe) = hybrid_score(HybridScoreInput {
            normalized_text_score: Some(0.8),
            normalized_vector_score: Some(0.5),
            freshness_score: Some(0.2),
        });

        assert_eq!(recipe, HybridScoringRecipe::DEFAULT);
        assert!((score - 0.635).abs() < f32::EPSILON);
        assert_eq!(
            recipe.label(),
            "score = 0.55 * normalized_text_score + 0.35 * normalized_vector_score + 0.10 * freshness_score"
        );
    }

    #[test]
    fn hybrid_score_gives_single_primary_source_full_weight() {
        let (text_score, text_recipe) = hybrid_score(HybridScoreInput {
            normalized_text_score: Some(0.7),
            normalized_vector_score: None,
            freshness_score: Some(1.0),
        });
        assert_eq!(text_score, 0.7);
        assert_eq!(text_recipe.text_weight, 1.0);
        assert_eq!(text_recipe.vector_weight, 0.0);
        assert_eq!(text_recipe.freshness_weight, 0.0);

        let (vector_score, vector_recipe) = hybrid_score(HybridScoreInput {
            normalized_text_score: None,
            normalized_vector_score: Some(0.6),
            freshness_score: Some(1.0),
        });
        assert_eq!(vector_score, 0.6);
        assert_eq!(vector_recipe.text_weight, 0.0);
        assert_eq!(vector_recipe.vector_weight, 1.0);
        assert_eq!(vector_recipe.freshness_weight, 0.0);
    }

    #[test]
    fn hybrid_score_clamps_inputs_to_normalized_range() {
        let (score, recipe) = hybrid_score(HybridScoreInput {
            normalized_text_score: Some(2.0),
            normalized_vector_score: Some(-1.0),
            freshness_score: Some(0.5),
        });

        assert_eq!(recipe, HybridScoringRecipe::DEFAULT);
        assert!((score - 0.60).abs() < f32::EPSILON);
    }
}
