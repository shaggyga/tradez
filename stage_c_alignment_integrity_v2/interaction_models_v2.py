from matched_campaign_models_v2 import contract as baseline_contract
from rich_family_models_v2 import frame,transform_state,validate_fit,predict,score_chunk,METHODS
from matched_view_models_v2 import fit_pair
from interaction_features_v2 import GROUPS,AXES,PAIRS

def contract():
    c=baseline_contract();c['baseline_features']=c.pop('features')
    return {**c,'schema_version':'forex_fixed_interaction_comparison.v1','groups':list(GROUPS),'methods':list(METHODS),
        'axes':list(AXES),'products':[list(x) for x in PAIRS],'marginal_screen_used':False,
        'preprocessing':'retained_train_only_imputation_and_scaling_on_original_plus_fixed_products',
        'baseline':'reuse_original_legacy26_models_forecasts_controls_without_refit',
        'hypothesis':'return_relationships_can_depend_on_slower_trend_volatility_and_session_age',
        'next_item':'operator_handoff_catalog_and_vault_navigation_v2'}
