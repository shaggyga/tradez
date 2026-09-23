from matched_campaign_models_v2 import contract as baseline_contract
from rich_family_models_v2 import frame,transform_state,validate_fit,predict,score_chunk,METHODS
from noise_control_features_v2 import GROUPS,SEEDS
from matched_view_models_v2 import fit_pair

def contract():
    c=baseline_contract();c['baseline_features']=c.pop('features')
    return {**c,'schema_version':'forex_noise_control_comparison.v1','groups':list(GROUPS),'methods':list(METHODS),'seeds':list(SEEDS),'nuisance_width':8,
        'preprocessing':'retained_median_imputation_missing_indicators_standard_scaler_train_only',
        'baseline':'reuse_original_legacy26_models_forecasts_controls_without_refit',
        'seed_selection':'two_predeclared_seeds_report_both_no_selection',
        'control_scope':'noise_only8_and_legacy26_plus8_on_same_original_eligible_support',
        'next_item':'conditional_interaction_positive_control_v2'}

