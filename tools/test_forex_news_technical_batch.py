import unittest
import forex_news_technical_batch as batch


class BatchTests(unittest.TestCase):
    def test_actor_pair_direction_and_reaction_exclusion(self):
        self.assertEqual(batch.policy_direction('Hawkish Fed puts euro on the ropes','EUR_USD'),-1)
        self.assertEqual(batch.policy_direction('EUR/USD rebounds from low','EUR_USD'),0)
        self.assertEqual(batch.policy_direction('UBS sees EUR/USD long opportunity','EUR_USD'),0)

    def test_conflicting_policy_and_equal_currency_signs_abstain(self):
        self.assertEqual(batch.policy_direction('Fed is hawkish while Fed is dovish','EUR_USD'),0)
        self.assertEqual(batch.policy_direction('Fed is hawkish while ECB is hawkish','EUR_USD'),0)
        self.assertEqual(batch.policy_direction('Less dovish ECB','EUR_USD'),1)

    def test_matched_counts_cost_sign_and_original_eligibility(self):
        r={'technical_direction':1,'original_direction':-1,'new_direction':-1,
           'original_publish_eligible':False,'outcomes':{'15':{'status':'available',
           'return_bps':-2,'long_net_bps':-3,'short_net_bps':1},'60':{'status':'missing_path'}}}
        metrics=batch.summarize([r])
        matched=[m for m in metrics if m['stratum']=='matched_original_new_technical']
        self.assertEqual(len(matched),3)
        self.assertTrue(all(m['n']==1 and m['horizon_minutes']==15 for m in matched))
        technical=next(m for m in matched if m['arm']=='technical')
        news=next(m for m in matched if m['arm']=='new_policy_interpretation')
        self.assertEqual((technical['direction_hit_rate'],news['direction_hit_rate']),(0,1))
        self.assertEqual(news['mean_net_bps'],1)
        self.assertFalse(any(m['stratum']=='original_article_publish_eligible' for m in metrics))

    def test_flat_return_not_counted_as_direction_hit(self):
        r={'technical_direction':1,'original_direction':0,'new_direction':0,
           'original_publish_eligible':False,'outcomes':{'5':{'status':'available',
           'return_bps':0,'long_net_bps':-1,'short_net_bps':-1}}}
        self.assertEqual(batch.summarize([r])[0]['direction_hit_rate'],0)


if __name__=='__main__':unittest.main()
