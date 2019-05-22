__INTERACTIONS = """WITH user_transaction_number AS (WITH ranked_transactions AS (SELECT user_id, id AS transaction_id, time, ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY TIME) AS transaction_rank FROM accountcoupon_couponusehistory)

select t1.user_id as user_id, t1.time as transaction_1, t2.time as transaction_2, t3.time as transaction_3, t10.time as transaction_10 , users.date_joined as date_joined from
(select * from ranked_transactions as t where t.transaction_rank=1) as t1 left join
(select * from ranked_transactions as t where t.transaction_rank=2) as t2 on t1.user_id = t2.user_id left join
(select * from ranked_transactions as t where t.transaction_rank=3) as t3 on t1.user_id = t3.user_id left join
(select * from ranked_transactions as t where t.transaction_rank=10) as t10 on t1.user_id = t10.user_id
left join account_localuser as users on users.id = t1.user_id)
SELECT 
	users.email  AS "users.email",
	users.id  AS "users.id",
	TO_CHAR(TO_DATE(CONVERT_TIMEZONE('UTC', 'Australia/Sydney', CAST(user_transaction_number.transaction_1  AS TIMESTAMP_NTZ))), 'YYYY-MM-DD') AS "user_transaction_number.transaction_1",
	TO_CHAR(TO_DATE(CONVERT_TIMEZONE('UTC', 'Australia/Sydney', CAST(user_transaction_number.transaction_2  AS TIMESTAMP_NTZ))), 'YYYY-MM-DD') AS "user_transaction_number.transaction_2",
	TO_CHAR(TO_DATE(CONVERT_TIMEZONE('UTC', 'Australia/Sydney', CAST(user_transaction_number.transaction_3  AS TIMESTAMP_NTZ))), 'YYYY-MM-DD') AS "user_transaction_number.transaction_3",
	TO_CHAR(TO_DATE(CONVERT_TIMEZONE('UTC', 'Australia/Sydney', CAST(user_transaction_number.transaction_10  AS TIMESTAMP_NTZ))), 'YYYY-MM-DD') AS "user_transaction_number.transaction_10",
	accountcoupon_couponusehistory.bill_amount  AS "accountcoupon_couponusehistory.bill_amount",
	CASE WHEN (case when (coupon_coupon.name LIKE '%Frenzy%') then accountcoupon_couponusehistory.saved_amount - accountcoupon_couponusehistory.commission else 0 end) + (case
          when accountcoupon_couponusehistory.special_promo_credit_id is not null then (
            case when promo_codes.credit  - accountcoupon_couponusehistory.bill_amount <= 0 then promo_codes.credit else promo_codes.credit - (promo_codes.credit - accountcoupon_couponusehistory.bill_amount) end
            )
          else 0 end) > 0  THEN 'Yes' ELSE 'No' END
 AS "accountcoupon_couponusehistory.is_inorganic",
	case when merchant_merchant.membership_zone_id = 1 then 'Melbourne'
          when merchant_merchant.membership_zone_id = 2 then 'Sydney'
          else null end AS "merchant_merchant.city",
	merchant_merchant.id  AS "merchant_merchant.id",
	merchant_merchant.name  AS "merchant_merchant.name",
	branch_branch.id  AS "branch_branch.id",
	TO_CHAR(TO_DATE(CONVERT_TIMEZONE('UTC', 'Australia/Sydney', CAST(accountcoupon_couponusehistory.time  AS TIMESTAMP_NTZ))), 'YYYY-MM-DD') AS "accountcoupon_couponusehistory.time_date",
	AVG(accountcoupon_couponusehistory.bill_amount ) AS "accountcoupon_couponusehistory.aov"
FROM account_localuser  AS users
LEFT JOIN accountcoupon_couponusehistory  AS accountcoupon_couponusehistory ON users.id = accountcoupon_couponusehistory.user_id 
LEFT JOIN branch_branch  AS branch_branch ON accountcoupon_couponusehistory.branch_id = branch_branch.id 
LEFT JOIN merchant_merchant  AS merchant_merchant ON merchant_merchant.id = branch_branch.merchant_id 
LEFT JOIN user_transaction_number ON users.id = user_transaction_number.user_id 
LEFT JOIN membershipvoucher_membershipvoucher  AS promo_codes ON accountcoupon_couponusehistory.special_promo_credit_id = promo_codes.id 
LEFT JOIN coupon_coupon  AS coupon_coupon ON accountcoupon_couponusehistory.coupon_id = coupon_coupon.id 

WHERE 
	NOT (accountcoupon_couponusehistory.bill_amount  IS NULL)
GROUP BY 1,2,TO_DATE(CONVERT_TIMEZONE('UTC', 'Australia/Sydney'
									, CAST(user_transaction_number.transaction_1  AS TIMESTAMP_NTZ)))
									,TO_DATE(CONVERT_TIMEZONE('UTC', 'Australia/Sydney', CAST(user_transaction_number.transaction_2  AS TIMESTAMP_NTZ)))
									,TO_DATE(CONVERT_TIMEZONE('UTC', 'Australia/Sydney', CAST(user_transaction_number.transaction_3  AS TIMESTAMP_NTZ)))
									,TO_DATE(CONVERT_TIMEZONE('UTC', 'Australia/Sydney', CAST(user_transaction_number.transaction_10  AS TIMESTAMP_NTZ)))
									,7,8,9,10,11,12,TO_DATE(CONVERT_TIMEZONE('UTC', 'Australia/Sydney', CAST(accountcoupon_couponusehistory.time  AS TIMESTAMP_NTZ)))
ORDER BY 3 DESC"""

__COORDINATES = """SELECT 
	branches.id  AS "branches.id",
	branch_coordinates."LATITUDE"  AS "branch_coordinates.latitude",
	branch_coordinates."LONGITUDE"  AS "branch_coordinates.longitude"
FROM merchant_merchant  AS merchant_merchant
LEFT JOIN branch_branch  AS branches ON merchant_merchant.id = branches.merchant_id 
LEFT JOIN POSTGRES.BRANCH_COORDINATES  AS branch_coordinates ON branches.id = (branch_coordinates."ID") 

GROUP BY 1,2,3
ORDER BY 1"""

__EMAILS = """SELECT 
	user_view.id  AS "user_view.id",
	user_view.email  AS "user_view.email"
FROM account_localuser AS user_view """

__USER_CITIES = """SELECT 
	users.id  AS "users.id",
	users.email  AS "users.email",
	case when merchant_merchant.membership_zone_id = 1 then 'Melbourne'
          when merchant_merchant.membership_zone_id = 2 then 'Sydney'
          else null end AS "merchant_merchant.city"
FROM account_localuser  AS users
LEFT JOIN accountcoupon_couponusehistory  AS accountcoupon_couponusehistory ON users.id = accountcoupon_couponusehistory.user_id 
LEFT JOIN branch_branch  AS branch_branch ON accountcoupon_couponusehistory.branch_id = branch_branch.id 
LEFT JOIN merchant_merchant  AS merchant_merchant ON merchant_merchant.id = branch_branch.merchant_id 

WHERE 
	(((case when merchant_merchant.membership_zone_id = 1 then 'Melbourne'
          when merchant_merchant.membership_zone_id = 2 then 'Sydney'
          else null end) IS NOT NULL))
GROUP BY 1,2,3"""

import sys

def get_query(kind=None):
	if kind=='interactions':
		return __INTERACTIONS

	elif kind=='coordinates':
		return __COORDINATES
	
	elif kind=='emails':
		return __EMAILS
	
	elif kind=='user_cities':
		return __USER_CITIES

	else:
		assert ValueError
		print("""Ill specified query. Select one of:\ninteractions
			;\nemails;\nuser_cities; or\ncoordinates""")
		sys.exit()