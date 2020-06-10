mod min_heap;

use min_heap::MinHeap;
use std::cmp;

pub type Balance = u128;
pub type ShardId = usize;
pub type AccountId = String;

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct ValidatorStake {
    account_id: AccountId,
    stake: Balance,
}

#[cfg(test)]
impl ValidatorStake {
    pub fn new(account_id: AccountId, stake: Balance) -> Self {
        Self { account_id, stake }
    }
}

/// Assign block producers (a.k.a validators) to shards. The i-th element
/// of the output corresponds to the validators assigned to the i-th shard.
/// This function ensures that every shard has at least `min_validators_per_shard`
/// assigned to it, and attempts to balance the stakes between shards (keep the total
/// stake assigned to each shard approximately equal). This function performs
/// best when the number of block producers is greater than
/// `num_shards * min_validators_per_shard`.
pub fn assign_shards(
    block_producers: Vec<ValidatorStake>,
    num_shards: usize,
    min_validators_per_shard: usize,
) -> Vec<Vec<ValidatorStake>> {
    // Initially, sort by number of validators, then total stake
    // (i.e. favour filling under-occupied shards first).
    let mut shard_index: MinHeap<(usize, Balance, ShardId)> =
        (0..num_shards).map(|s| (0, 0, s)).collect();

    let num_block_producers = block_producers.len();
    if num_block_producers < min_validators_per_shard {
        panic!("Not enough block producers to minimally fill shards");
    }
    let required_validator_count =
        cmp::max(num_block_producers, num_shards * min_validators_per_shard);
    let mut bp_iter = block_producers
        .into_iter()
        .cycle()
        .enumerate()
        .take(required_validator_count);

    let mut result: Vec<Vec<ValidatorStake>> = (0..num_shards).map(|_| Vec::new()).collect();

    // Place validators into shards while there are still some without the
    // minimum required number.
    while shard_index.peek().unwrap().0 < min_validators_per_shard {
        let (assignment_index, bp) = bp_iter
            .next()
            .expect("bp_iter should contain enough elements to minimally fill each shard");
        let (least_validator_count, shard_stake, shard_id) = shard_index
            .pop()
            .expect("shard_index should never be empty");

        if assignment_index < num_block_producers {
            // no need to worry about duplicates yet; still on first pass through validators
            shard_index.push((least_validator_count + 1, shard_stake + bp.stake, shard_id));
            result[shard_id].push(bp);
        } else if result[shard_id].contains(&bp) {
            // `bp` is already assigned to this shard, need to assign elsewhere

            // Tracks shards `bp` is already in, these will need to be pushed back into
            // shard_index when we eventually assign `bp`.
            let mut buffer = Vec::new();
            buffer.push((least_validator_count, shard_stake, shard_id));
            loop {
                // We can still expect that there exists a shard bp has not been assigned to
                // because we check above that there are at least `min_validators_per_shard` distinct
                // block producers. This means the worst case scenario is in the end every block
                // producer is assigned to every shard, in which case we would not be trying to
                // assign a block producer right now.
                let (least_validator_count, shard_stake, shard_id) = shard_index
                    .pop()
                    .expect("shard_index should never be empty");
                if result[shard_id].contains(&bp) {
                    buffer.push((least_validator_count, shard_stake, shard_id))
                } else {
                    shard_index.push((least_validator_count + 1, shard_stake + bp.stake, shard_id));
                    result[shard_id].push(bp);
                    break;
                }
            }
            for tuple in buffer {
                shard_index.push(tuple);
            }
        } else {
            shard_index.push((least_validator_count + 1, shard_stake + bp.stake, shard_id));
            result[shard_id].push(bp);
        }
    }

    let mut bp_iter = bp_iter.peekable();
    if bp_iter.peek().is_some() {
        // still more validators left to assign

        // re-index shards to favour lowest stake first
        // (i.e. prefer balanced stakes to equal validator count)
        let mut shard_index: MinHeap<(Balance, usize, ShardId)> = shard_index
            .into_iter()
            .map(|(validator_count, stake, shard_id)| (stake, validator_count, shard_id))
            .collect();

        // In this case we do not need to worry about duplicate
        // assignment because it can only happen if
        // num_block_producers > (min_validators_per_shard * num_shards)
        // (i.e. we will not loop over the block_producers multiple times).
        for (_, bp) in bp_iter {
            let (least_stake, validator_count, shard_id) = shard_index
                .pop()
                .expect("shard_index should never be empty");
            shard_index.push((least_stake + bp.stake, validator_count + 1, shard_id));
            result[shard_id].push(bp);
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::{assign_shards, ValidatorStake};
    use std::cmp;
    use std::collections::HashSet;

    const EXPONENTIAL_STAKES: [u128; 12] = [100, 90, 81, 73, 66, 59, 53, 48, 43, 39, 35, 31];

    #[test]
    fn test_exponential_distribution_few_shards() {
        // algorithm works well when there are few shards relative to the number of block producers
        test_exponential_distribution_common(3, 3);
    }

    #[test]
    fn test_exponential_distribution_several_shards() {
        // algorithm performs less well when there are more shards
        test_exponential_distribution_common(6, 13);
    }

    #[test]
    fn test_exponential_distribution_many_shards() {
        // algorithm performs even worse when there are many shards
        test_exponential_distribution_common(24, 41);
    }

    #[test]
    #[should_panic]
    fn test_not_enough_validators() {
        let stakes = &[100];
        let block_producers = make_validators(stakes);
        let num_shards = 1;
        let min_validators_per_shard = 3; // one validator cannot fill 3 slots
        assign_shards(block_producers, num_shards, min_validators_per_shard);
    }

    #[test]
    fn test_step_distribution_shards() {
        let num_shards = 2;
        let stakes = &[100, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10];
        let block_producers = make_validators(stakes);
        let min_validators_per_shard = 2;

        let assignment = assign_shards(block_producers, num_shards, min_validators_per_shard);

        // The algorithm ensures the minimum number of validators is present
        // in each shard, even if it makes the stakes more uneven.
        let shard_0 = assignment.first().unwrap();
        assert_eq!(shard_0.len(), min_validators_per_shard);
        let stake_0 = shard_0.iter().map(|bp| bp.stake).sum::<u128>();
        assert_eq!(stake_0, 110);

        let shard_1 = assignment.last().unwrap();
        assert_eq!(shard_1.len(), stakes.len() - min_validators_per_shard);
        let stake_1 = shard_1.iter().map(|bp| bp.stake).sum::<u128>();
        assert_eq!(stake_1, 90);
    }

    fn test_exponential_distribution_common(num_shards: usize, diff_tolerance: i128) {
        let stakes = &EXPONENTIAL_STAKES;
        let block_producers = make_validators(stakes);
        let min_validators_per_shard = 2;

        let validators_per_shard =
            cmp::max(block_producers.len() / num_shards, min_validators_per_shard);
        let average_stake_per_shard =
            (validators_per_shard as u128) * stakes.iter().sum::<u128>() / (stakes.len() as u128);
        let assignment = assign_shards(block_producers, num_shards, min_validators_per_shard);

        // validator distribution should be even
        assert!(assignment
            .iter()
            .all(|bps| bps.len() == validators_per_shard));

        // no validator should be assigned to the same shard more than once
        assert!(assignment
            .iter()
            .all(|bps| bps.iter().collect::<HashSet<_>>().len() == bps.len()));

        // stake distribution should be even
        assert!(assignment.iter().all(|bps| {
            let shard_stake = bps.iter().map(|bp| bp.stake).sum::<u128>();
            let stake_diff: i128 = (shard_stake as i128) - (average_stake_per_shard as i128);
            stake_diff.abs() < diff_tolerance
        }));
    }

    fn make_validators(stakes: &[u128]) -> Vec<ValidatorStake> {
        let names = validator_names(stakes.len());

        stakes
            .iter()
            .zip(names.into_iter())
            .map(|(stake, name)| ValidatorStake::new(name, *stake))
            .collect()
    }

    fn validator_names(size: usize) -> Vec<String> {
        (b'A'..b'Z')
            .take(size)
            .map(|n| {
                let mut s = String::with_capacity(1);
                s.push(n as char);
                s
            })
            .collect()
    }
}
