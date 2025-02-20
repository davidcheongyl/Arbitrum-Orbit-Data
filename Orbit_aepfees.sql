/*
-- Script is for use in Allium Web app https://app.allium.so/explorer

--this version calculates l1basefee using the fees paid to post batches on Orbits parent chain 

script sections
- cte containing prices of non eth gas tokens, converted to eth (ending at token_to_eth_prices cte)
- cte contaning relevant txs used to calculate l1basefee (cost of posting batches to orbit's parent chain) and also assertions  (ending at raw_txs_forl1basefee cte)
- ctes containing  relevant l2fees for different orbit chains (there are several ctes, one for each chain)
- ctes containing relevant orbit assertion costs (cost of posting assertions onto orbit's parent chain)  - (there are several ctes, rn 2, for each parent chain where orbits settle to)

*/

WITH daily_prices AS (
    -- Step 1: Select token and ETH prices
    SELECT 
        date_trunc('day', timestamp) AS day, 
        symbol, 
        AVG(price) AS avg_price_usd
    FROM common.prices.hourly
    WHERE (
        (address IN ('0x4ed4e862860bed51a9570b96d89af5e1b0efefed') AND chain ='base' AND source='provider_a') -- DEGEN on Base
       OR  (address IN ('0x9c7beba8f6ef6643abd725e45a4e8387ef260649') AND chain ='ethereum' AND source='provider_a') -- Gravity on Ethereum
        OR (lower(symbol) = 'azero' AND address ='aleph-zero' AND source='provider_a') -- AlephZero
        OR (symbol = 'eth' AND chain = 'ethereum' AND address='0x0000000000000000000000000000000000000000' AND source='provider_a') -- ETH
    )
    AND timestamp >= timestamp '2024-02-01'
    GROUP BY day, symbol
),
eth_prices AS (
    -- Step 2: Filter ETH prices
    SELECT 
        day, 
        avg_price_usd AS eth_price_usd
    FROM daily_prices
    WHERE symbol = 'eth'
),
token_prices AS (
    -- Step 3: Filter token prices except ETH
    SELECT 
        day, 
        symbol, 
        avg_price_usd AS token_price_usd
    FROM daily_prices
    WHERE symbol != 'eth'
),
token_to_eth_prices AS (
    -- Step 4: Join token prices with ETH prices to calculate token to ETH conversion
    SELECT 
        tp.day, 
        upper(tp.symbol) as symbol, 
        tp.token_price_usd, 
        ep.eth_price_usd, 
        (tp.token_price_usd / ep.eth_price_usd) AS token_price_eth
    FROM token_prices tp
    JOIN eth_prices ep
    ON tp.day = ep.day
),

raw_txs_forl1basefee_and_assertions as  -- this cte is to prefilter the raw txs data for parent chains, used as part of the calculation the l1basefee (settlement) and also the assertions
  (
  
select hash,block_timestamp,from_address,to_address,receipt_effective_gas_price, receipt_gas_used, null  as receipt_l1_fee ,  input , substring(input,1,10)  as method_id ,'ethereum' as parentchain_flag
    --, case when substring(input,1,10) = '0x8f111f3c' or substring(input,1,10) =  '0x3e5aa082' or substring(input,1,10)= '0xe0bc9729' then 'l1basefee' else 'assertions' end as flag 
from ethereum.raw.transactions t

WHERE
      block_timestamp > TIMESTAMP '2023-12-01' -- Shared block_timestamp condition
       AND block_number > 18687851
      AND (
        -- First set of conditions: Fees used to pay for posting batches 
        (
          SUBSTRING(input, 1, 10) IN ('0x8f111f3c', '0x3e5aa082', '0xe0bc9729')
          AND to_address IN (      -- these are the sequencerinbox addresses
            '0x6ca2a628fb690bd431f4aa608655ce37c66aff9d', --reya
            '0x51c4a227d59e49e26ea07d8e4e9af163da4c87a0', --real
            '0xf75206c49c1694594e3e69252e519434f1579876',
            '0xf4ef823d57819ac7202a081a5b49376bd28e7b3a',
            '0x8d99372612e8cfe7163b1a453831bc40eaeb3cf3',
            '0xb7d188eb30e7984f93bec34ee8b45a148bd594c6'
          )
        )
        OR
        -- Second set of conditions: Fees paid to create and/or confirm assertions
        (
            to_address IN (              --these are the rollupproxy sc addresses for Orbits of interest
       
            '0x5073da9ca4810f3e0aa01c20c7d9d02c3f522e11', -- kinto
            '0x448bbd134de1b23976073ab4f2915849b2dcd73a', -- reya
            '0xf993af239770932a0edab88b6a5ba3708bd58239', -- gravity
            -- '0x6594085ca55a2b3a5fad1c57a270d060eea99877', -- parallel
            '0xc4f7b37be2bbbcf07373f28c61b1a259dfe49d2a', -- real
            '0x1ca12290d954cfe022323b6a6df92113ed6b1c98', -- alephzero
            '0x6fa8b24c85409a4fcb541c9964766862aa007f39' -- alienx
          )
        )
      ) 


  
/*union all
  
select hash,block_timestamp,from_address,to_address,receipt_effective_gas_price, receipt_gas_used,receipt_l1_fee, input , substring(input,1,10)  as method_id , 'base' as parentchain_flag
  -- ,  case when substring(input,1,10) = '0x8f111f3c' or substring(input,1,10) =  '0x3e5aa082' or substring(input,1,10)= '0xe0bc9729' then 'l1basefee' else 'assertions' end as flag 
from base.raw.transactions t 

  
where 
  
     (
        --first block of code  for fees used to pay for posting batches on parent chain (l1basefee)
        (block_timestamp>timestamp  '2024-02-01' and block_number>=1
        and substring(input,1,10) in ( '0x8f111f3c' , '0x3e5aa082', '0xe0bc9729')
        and to_address in ('0x6216dd1ee27c5acec7427052d3ecdc98e2bc2221'))
       --2nd block of code  for fees paid to create and/or confirm assertions
        OR
      ( block_timestamp>timestamp  '2024-02-01' and block_number>=1  
        and to_address='0xd34f3a11f10db069173b32d84f02eda578709143' --these are the rollupproxy sc addresses for Orbits of interest
       
      )
    )
  */
  
  ),

-- Other fees traces as you already defined
l2fees_trace_kinto AS (

  select transaction_hash, bet_from_addres as from_address ,bet_purpose,bet_value,aet_to_addres as to_address_fees , aet_purpose,aet_value_hex,aet_value,  date_trunc('day', block_timestamp) as block_dt
  ,  aet_value/POWER(10,18) as fee_value_dec
  from kinto.raw.traces_evm_transfers 
  where aet_purpose='feeCollection'

),

/*l1fees_trace_kinto AS (
  SELECT
    'kinto' AS chain,
  date_trunc('day',block_timestamp) as block_dt,
    SUM(CASE WHEN to_address = '0x09d34b74cd8b1c4394a3cd9630e1ba027e6ed4f5' THEN value/POWER(10,18) END) AS l1_surplus_fee,
    SUM(CASE WHEN to_address = '0xe27f3f6db6824def1738b2aace2672ac59046a39' THEN value/POWER(10,18) END) AS l1_base_fee
  FROM
    kinto.raw.traces
  WHERE
    from_address IN ('0xa4b00000000000000000000000000000000000f6')
    group by date_trunc('day',block_timestamp)
), */

l2fees_trace_real AS (

  select transaction_hash, bet_from_addres as from_address ,bet_purpose,bet_value,aet_to_addres as to_address_fees , aet_purpose,aet_value_hex,aet_value,  date_trunc('day', block_timestamp) as block_dt
  ,  aet_value/POWER(10,18) as fee_value_dec
  from real.raw.traces_evm_transfers 
    where aet_purpose='feeCollection'

  
),

/*l1fees_trace_real AS (
  SELECT
    'real' AS chain,
    date_trunc('day',block_timestamp) as block_dt,
    SUM(CASE WHEN to_address = '0xbb0385febfd25e01527617938129a34bd497331e' THEN value/POWER(10,18) END) AS l1_surplus_fee,
    SUM(CASE WHEN to_address = '0x0e00df1afc8574762ac4c4d8e5d1a19bd6a8fa2e' THEN value/POWER(10,18) END) AS l1_base_fee
  FROM
    real.raw.traces
  WHERE
    from_address IN ('0xa4b00000000000000000000000000000000000f6')
    group by date_trunc('day',block_timestamp)
),*/

l2fees_trace_degen AS (
   select transaction_hash, bet_from_addres as from_address ,bet_purpose,bet_value,aet_to_addres as to_address_fees , aet_purpose,aet_value_hex,aet_value,  date_trunc('day', block_timestamp) as block_dt
  ,  aet_value/POWER(10,18) as fee_value_dec
  from degen.raw.traces_evm_transfers 
    where aet_purpose='feeCollection'
),

/*l1fees_trace_degen AS (
  SELECT
    'degenchain' AS chain,
     date_trunc('day',block_timestamp) as block_dt,
    SUM(CASE WHEN to_address = '0xa3883ee393b03b6711962843b6844c9027a3cc8f' THEN value/POWER(10,18) END) AS l1_surplus_fee,
    SUM(CASE WHEN to_address = '0xa3582189403f67a9cdb1ce0ac066c954ffd3f205' THEN value/POWER(10,18) END) AS l1_base_fee
  FROM
    degen.raw.traces
  WHERE
    from_address IN ('0xa4b00000000000000000000000000000000000f6')
   
  group by date_trunc('day',block_timestamp)
),*/
  
l2fees_trace_alephzero AS (
    select transaction_hash, bet_from_addres as from_address ,bet_purpose,bet_value,aet_to_addres as to_address_fees , aet_purpose,aet_value_hex,aet_value,  date_trunc('day', block_timestamp) as block_dt
  ,  aet_value/POWER(10,18) as fee_value_dec
  from aleph_zero.raw.traces_evm_transfers 
    where aet_purpose='feeCollection'
),

/*l1fees_trace_alephzero AS (
  SELECT
    'alephzero' AS chain,
    date_trunc('day',block_timestamp) as block_dt,
    SUM(CASE WHEN to_address = '0x257812604076712675ae9788f5bd738173ca3ce0' THEN value/POWER(10,18) END) AS l1_surplus_fee,
    SUM(CASE WHEN to_address = '0xa4b000000000000000000073657175656e636572' THEN value/POWER(10,18) END) AS l1_base_fee
  FROM
    aleph_zero.raw.traces
  WHERE
    from_address IN ('0xa4b00000000000000000000000000000000000f6')
  group by date_trunc('day',block_timestamp)
),*/

  l2fees_trace_gravity AS (
  
       select transaction_hash, bet_from_addres as from_address ,bet_purpose,bet_value,aet_to_addres as to_address_fees , aet_purpose,aet_value_hex,aet_value,  date_trunc('day', block_timestamp) as block_dt
  ,  aet_value/POWER(10,18) as fee_value_dec
  from gravity.raw.traces_evm_transfers 
    where aet_purpose='feeCollection'

),

l2fees_trace_reya AS (
     select transaction_hash, bet_from_addres as from_address ,bet_purpose,bet_value,aet_to_addres as to_address_fees , aet_purpose,aet_value_hex,aet_value,  date_trunc('day', block_timestamp) as block_dt
  ,  aet_value/POWER(10,18) as fee_value_dec
  from reya.raw.traces_evm_transfers 
    where aet_purpose='feeCollection'

),

l2fees_trace_alienx AS (
     select transaction_hash, bet_from_addres as from_address ,bet_purpose,bet_value,aet_to_addres as to_address_fees , aet_purpose,aet_value_hex,aet_value,  date_trunc('day', block_timestamp) as block_dt
  ,  aet_value/POWER(10,18) as fee_value_dec
  from  alienx.raw.traces_evm_transfers 
    where aet_purpose='feeCollection'

),

orbit_assertions_eth as 
(
  select address
 -- , from_address
  --,to_address
  ,date_trunc('day',block_timestamp)  as block_dt
  ,transaction_hash
  

  from ethereum.raw.logs
   where topic0='0x4f4caa9e67fb994e349dd35d1ad0ce23053d4323f83ce11dc817b5435031d096' -- node created
  -- '0x22ef0479a7ff660660d1c2fe35f1b632cf31675c2d9378db8cec95b00d8ffa3c' -- node confirmed
  and block_timestamp>timestamp '2023-12-01' and block_number>18687851
  and address in ('0x5073da9ca4810f3e0aa01c20c7d9d02c3f522e11','0x448bbd134de1b23976073ab4f2915849b2dcd73a', --these are the rollupproxy sc addresses for Orbits of interest
                    '0xf993af239770932a0edab88b6a5ba3708bd58239','0x6594085ca55a2b3a5fad1c57a270d060eea99877',
                    '0xc4f7b37be2bbbcf07373f28c61b1a259dfe49d2a', '0x1ca12290d954cfe022323b6a6df92113ed6b1c98' 
                    ,'0x6fa8b24c85409a4fcb541c9964766862aa007f39')
  --and  transaction_hash in ('0x02de4a50c4394e00e9a00e7bad70ff1944c949f7d92fcc5a7a2ba213beab5397','0xd3690d476a576005e075890865ab9f8397c0eca50fa0cee439e048f0ba0000ff')               

),

orbit_assertions_eth_totals as (
select 
  case when address ='0x5073da9ca4810f3e0aa01c20c7d9d02c3f522e11' then 'kinto'
          when address ='0x448bbd134de1b23976073ab4f2915849b2dcd73a' then 'reya'
           when address ='0xf993af239770932a0edab88b6a5ba3708bd58239' then 'gravity' 
           when address ='0x6594085ca55a2b3a5fad1c57a270d060eea99877' then 'parallel' 
        when address ='0xc4f7b37be2bbbcf07373f28c61b1a259dfe49d2a' then 'real' 
        when address ='0x1ca12290d954cfe022323b6a6df92113ed6b1c98' then 'alephzero'  
         when address ='0x6fa8b24c85409a4fcb541c9964766862aa007f39' then 'alienx'  
   end as orbit_rollupproxy_flag                                                             --these are the rollupproxy sc addresses for Orbits of interest
  , block_dt
  , address
  , sum((receipt_effective_gas_price*receipt_gas_used)/power(10,18))  as tx_fee_assertions -- fees paid to post assertions
   , 'eth' as assertions_costs_units
  --, count(distinct transaction_hash) as tx
  --, min(block_timestamp) as earliest_dt
 -- , max(block_timestamp) as latest_dt
  from orbit_assertions_eth a 
  left join 
   (select   hash  , block_timestamp ,from_address,to_address,receipt_effective_gas_price,receipt_gas_used 
            from raw_txs_forl1basefee_and_assertions
            where method_id NOT IN ('0x8f111f3c', '0x3e5aa082', '0xe0bc9729') --these are batchposting method_id -- flag='l1basefee' 
            and parentchain_flag='ethereum' )  t on a.transaction_hash=t.hash 
group by 1,2,3
  )

/*, degen_assertions_base as 
(
  select --* 
  address
  ,date_trunc('day',block_timestamp) as block_dt
  ,transaction_hash

  from base.raw.logs
   where topic0='0x4f4caa9e67fb994e349dd35d1ad0ce23053d4323f83ce11dc817b5435031d096' -- node created  -- '0x22ef0479a7ff660660d1c2fe35f1b632cf31675c2d9378db8cec95b00d8ffa3c' -- node confirmed
  and address in ('0xd34f3a11f10db069173b32d84f02eda578709143') -- degen's Rollupproxy SC address
  -- and transaction_hash='0xa21013646183eba5709f0606d71c449822b834fa5a76c1e6cbef06cb9c0dd9d3'
  and block_timestamp>timestamp '2024-02-01'

),

degen_assertions_eth_totals as (
select 

   'degenchain' as orbit_rollupproxy_flag
  , block_dt
   ,address
  , sum((receipt_effective_gas_price*receipt_gas_used)/power(10,18)+receipt_l1_fee/power(10,18) )  as tx_fee_assertions
  , 'eth' as assertions_costs_units
  --,count(distinct transaction_hash) as tx
  --,min(a.block_timestamp) earliest_dt 
  --,max(a.block_timestamp) latest_dt

from degen_assertions_base a 
left join ( select   hash  , block_timestamp ,from_address,to_address,receipt_effective_gas_price,receipt_gas_used ,receipt_l1_fee
            from raw_txs_forl1basefee_and_assertions
            where method_id NOT IN ('0x8f111f3c', '0x3e5aa082', '0xe0bc9729') --these are batchposting method_id -- flag='l1basefee'
            and parentchain_flag='base'
          ) t on a.transaction_hash=t.hash 
--where t.hash='0xa21013646183eba5709f0606d71c449822b834fa5a76c1e6cbef06cb9c0dd9d3' -- 0.000002319906171739 eth
  group by block_dt,address
)*/

-- Final query
SELECT 
    a.orbitchain,
     a.block_dt as block_date,
    a.gas_token,
    
    -- Convert l2fees to ETH for chains that need it (AlephZero, Degenchain), leave others unchanged
   CASE 
        WHEN a.orbitchain IN ('alephzero', 'degenchain','gravity') THEN a.l2fees * t.token_price_eth 
        ELSE a.l2fees 
    END AS l2fees_eth,

    -- Convert l1_surplus_fee to ETH for chains that need it
  /*  CASE 
        WHEN a.orbitchain IN ('alephzero', 'degenchain') THEN b.l1_surplus_fee * t.token_price_eth 
        ELSE b.l1_surplus_fee 
    END AS l1_surplus_fee_eth,*/

    -- Convert l1_base_fee to ETH for chains that need it ; for chains that settle to Ethereum, they dont need to be converted for thel1basefee calcs as they are already in eth
    CASE 
        WHEN a.orbitchain IN  ('degenchain') THEN b.l1_base_fee * t.token_price_eth 
        ELSE b.l1_base_fee 
    END AS l1_base_fee_eth,

    -- Calculate gross revenue (sum of fees in ETH)
    (COALESCE((CASE 
                WHEN a.orbitchain IN ('alephzero', 'degenchain','gravity') THEN a.l2fees * t.token_price_eth 
                ELSE a.l2fees 
              END),0)  
      /* + 
    CASE 
        WHEN a.orbitchain IN ('alephzero', 'degenchain') THEN b.l1_surplus_fee * t.token_price_eth 
        ELSE b.l1_surplus_fee 
    END*/
      + 
    COALESCE((CASE 
                WHEN a.orbitchain IN ('degenchain') THEN b.l1_base_fee * t.token_price_eth  -- for chains that settle to Ethereum, they dont need to be converted for thel1basefee calcs as they are already in eth
                ELSE b.l1_base_fee 
              END),0)
    ) AS gross_revenue_eth,

    -- Net revenue excluding L1 base fees (in ETH)
    COALESCE((CASE 
                WHEN a.orbitchain IN ('alephzero', 'degenchain','gravity') THEN a.l2fees * t.token_price_eth 
                ELSE a.l2fees 
              END /* + 
              CASE 
                  WHEN a.orbitchain IN ('alephzero', 'degenchain') THEN b.l1_surplus_fee * t.token_price_eth 
                  ELSE b.l1_surplus_fee 
              END*/
                ),0) AS net_revenue_noassertions_eth

   , 0.1*net_revenue_noassertions_eth as aep_fees_noassertions 
  , assertions_costs_units
  , coalesce(assertions.tx_fee_assertions,0) as tx_fee_assertions
  , net_revenue_noassertions_eth-coalesce(assertions.tx_fee_assertions,0)  as net_revenue
  , 0.1*net_revenue as aep_fees

FROM (
    -- L2 fees data for all chains

    select to_address_fees , block_dt, sum(fee_value_dec) as l2fees ,'ETH' as gas_token, 'kinto' as orbitchain 
    from l2fees_trace_kinto 
    where to_address_fees !='0xa4b00000000000000000000000000000000000f6' -- remove this as this is where l1fees flow to initially, we are only interested in l2   
    group by to_address_fees,block_dt
  
    union all
  
    select to_address_fees, block_dt , sum(fee_value_dec) as l2fees,'ETH' as gas_token, 'real' as orbitchain
    from l2fees_trace_real
    where to_address_fees !='0xa4b00000000000000000000000000000000000f6'
    group by to_address_fees,block_dt
  
    union all
  
    select to_address_fees, block_dt , sum(fee_value_dec) as l2fees ,'DEGEN' as gas_token, 'degenchain' as orbitchain 
    from l2fees_trace_degen
    where  to_address_fees !='0xa4b00000000000000000000000000000000000f6' 
       AND to_address_fees !='0xa3883ee393b03b6711962843b6844c9027a3cc8f'   -- remove this address as the feecollection traces to this was only for setting up the chain on 3 txs (setL1PricingRewardRate, setMinimumL2BaseFee etc..)
    group by to_address_fees,block_dt
  
    union all
  
    SELECT to_address_fees, block_dt , SUM(fee_value_dec) AS l2fees, 'AZERO' AS gas_token, 'alephzero' AS orbitchain 
    FROM l2fees_trace_alephzero
    WHERE to_address_fees != '0xa4b00000000000000000000000000000000000f6'
    GROUP BY to_address_fees,block_dt

    union all
  
    SELECT to_address_fees, block_dt , SUM(fee_value_dec) AS l2fees, 'G' AS gas_token, 'gravity' AS orbitchain 
    FROM l2fees_trace_gravity
    WHERE to_address_fees != '0xa4b00000000000000000000000000000000000f6'
    GROUP BY to_address_fees,block_dt

    union all
  
    SELECT to_address_fees, block_dt , SUM(fee_value_dec) AS l2fees, 'ETH' AS gas_token, 'reya' AS orbitchain 
    FROM l2fees_trace_reya
    WHERE to_address_fees != '0xa4b00000000000000000000000000000000000f6'
    GROUP BY to_address_fees,block_dt

   union all
  
    SELECT to_address_fees, block_dt , SUM(fee_value_dec) AS l2fees, 'ETH' AS gas_token, 'alienx' AS orbitchain 
    FROM l2fees_trace_alienx
    WHERE to_address_fees != '0xa4b00000000000000000000000000000000000f6'
    GROUP BY to_address_fees,block_dt

  
) a

LEFT JOIN (
    -- L1 fees data for all chains
  
  /*select chain, block_dt, l1_surplus_fee,l1_base_fee
  from l1fees_trace_kinto 
  union all
  select chain,block_dt, l1_surplus_fee,l1_base_fee
  from  l1fees_trace_real
  union all
  select chain,block_dt,l1_surplus_fee,l1_base_fee
  from l1fees_trace_degen
  union all
  SELECT chain,block_dt, l1_surplus_fee, l1_base_fee
  FROM l1fees_trace_alephzero*/

  select  case when to_address = '0x6ca2a628fb690bd431f4aa608655ce37c66aff9d' then 'reya' 
               when to_address = '0x51c4a227d59e49e26ea07d8e4e9af163da4c87a0' then 'real' 
               when to_address= '0xf75206c49c1694594e3e69252e519434f1579876' then 'alephzero' 
              when to_address= '0xf4ef823d57819ac7202a081a5b49376bd28e7b3a' then 'kinto' 
              when to_address= '0x8d99372612e8cfe7163b1a453831bc40eaeb3cf3' then 'gravity'   
               when to_address= '0xb7d188eb30e7984f93bec34ee8b45a148bd594c6' then 'alienx'   
              when to_address = '0x6216dd1ee27c5acec7427052d3ecdc98e2bc2221' then 'degenchain' else 'others' end  as chain --these are the contract addresses of each orbit's sequencer inbox 
 , to_address
  , date_trunc('day',block_timestamp) as block_dt
--, substring(input,1,10)  as methodid

, sum(receipt_effective_gas_price*receipt_gas_used/power(10,18)) as l1_base_fee --total_tx_fee_eth

  
from raw_txs_forl1basefee_and_assertions
where  method_id IN ('0x8f111f3c', '0x3e5aa082', '0xe0bc9729') --these are batchposting method_id -- flag='l1basefee'
group by 1,2 ,3

) b ON a.orbitchain = b.chain AND a.block_dt = b.block_dt 

-- Join with token_to_eth_prices for conversions, only for the chains that need it
LEFT JOIN token_to_eth_prices t ON a.gas_token = t.symbol AND a.block_dt = t.day


LEFT JOIN
  
(
  select *
  from orbit_assertions_eth_totals
 /* union all
  select *
  from degen_assertions_eth_totals */
  
) assertions
on a.orbitchain=assertions.orbit_rollupproxy_flag and a.block_dt=assertions.block_dt
