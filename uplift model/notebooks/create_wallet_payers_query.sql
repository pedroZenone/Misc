CREATE MULTISET VOLATILE TABLE WALLET_PAYERS, NO LOG AS
(
SELECT	A.CUS_CUST_ID_BUY AS CUS_CUST_ID,
				COUNT(CASE
				WHEN TO_CHAR(A.PAY_MOVE_DATE, 'YYYYMM') = '201812' THEN A.PAY_PAYMENT_ID
				END) AS PAYMENTS_M3,
				COUNT(CASE
				WHEN TO_CHAR(A.PAY_MOVE_DATE, 'YYYYMM') = '201901' THEN A.PAY_PAYMENT_ID
				END) AS PAYMENTS_M2,
				COUNT(CASE
				WHEN TO_CHAR(A.PAY_MOVE_DATE, 'YYYYMM') = '201902' THEN A.PAY_PAYMENT_ID
				END) AS PAYMENTS_M1,
				COUNT(DISTINCT CASE
				WHEN TO_CHAR(A.PAY_MOVE_DATE, 'YYYYMM') = '201812' THEN A.PAY_MOVE_DATE
				END) AS DAYS_W_PAYMENTS_M3,
				COUNT(DISTINCT CASE
				WHEN TO_CHAR(A.PAY_MOVE_DATE, 'YYYYMM') = '201901' THEN A.PAY_MOVE_DATE
				END) AS DAYS_W_PAYMENTS_M2,
				COUNT(DISTINCT CASE
				WHEN TO_CHAR(A.PAY_MOVE_DATE, 'YYYYMM') = '201902' THEN A.PAY_MOVE_DATE
				END) AS DAYS_W_PAYMENTS_M1,
				COUNT(CASE
				WHEN TO_CHAR(A.PAY_MOVE_DATE, 'YYYYMM') = '201812' AND TPV_SEGMENT_DETAIL = 'Cellphone Recharge' THEN A.PAY_PAYMENT_ID
				END) AS PAYMENTS_CP_R_M3,
				COUNT(CASE
				WHEN TO_CHAR(A.PAY_MOVE_DATE, 'YYYYMM') = '201901' AND TPV_SEGMENT_DETAIL = 'Cellphone Recharge' THEN A.PAY_PAYMENT_ID
				END) AS PAYMENTS_CP_R_M2,
				COUNT(CASE
				WHEN TO_CHAR(A.PAY_MOVE_DATE, 'YYYYMM') = '201902' AND TPV_SEGMENT_DETAIL = 'Cellphone Recharge' THEN A.PAY_PAYMENT_ID
				END) AS PAYMENTS_CP_R_M1,
				COUNT(CASE
				WHEN TO_CHAR(A.PAY_MOVE_DATE, 'YYYYMM') = '201812' AND ZEROIFNULL(A.PAY_COUPON_AMOUNT_AMT) = 0 THEN A.PAY_PAYMENT_ID
				END) AS PAYMENTS_NO_DISC_M3,
				COUNT(CASE
				WHEN TO_CHAR(A.PAY_MOVE_DATE, 'YYYYMM') = '201901' AND ZEROIFNULL(A.PAY_COUPON_AMOUNT_AMT) = 0 THEN A.PAY_PAYMENT_ID
				END) AS PAYMENTS_NO_DISC_M2,
				COUNT(CASE
				WHEN TO_CHAR(A.PAY_MOVE_DATE, 'YYYYMM') = '201902' AND ZEROIFNULL(A.PAY_COUPON_AMOUNT_AMT) = 0 THEN A.PAY_PAYMENT_ID
				END) AS PAYMENTS_NO_DISC_M1,
				COUNT(CASE
				WHEN TO_CHAR(A.PAY_MOVE_DATE, 'YYYYMM') = '201812' AND TPV_SEGMENT_DETAIL = 'Cellphone Recharge' AND ZEROIFNULL(A.PAY_COUPON_AMOUNT_AMT) = 0 THEN A.PAY_PAYMENT_ID
				END) AS PAYMENTS_NO_DISC_CP_R_M3,
				COUNT(CASE
				WHEN TO_CHAR(A.PAY_MOVE_DATE, 'YYYYMM') = '201901' AND TPV_SEGMENT_DETAIL = 'Cellphone Recharge' AND ZEROIFNULL(A.PAY_COUPON_AMOUNT_AMT) = 0 THEN A.PAY_PAYMENT_ID
				END) AS PAYMENTS_NO_DISC_CP_R_M2,
				COUNT(CASE
				WHEN TO_CHAR(A.PAY_MOVE_DATE, 'YYYYMM') = '201902' AND TPV_SEGMENT_DETAIL = 'Cellphone Recharge' AND ZEROIFNULL(A.PAY_COUPON_AMOUNT_AMT) = 0 THEN A.PAY_PAYMENT_ID
				END) AS PAYMENTS_NO_DISC_CP_R_M1,
				MIN(CASE WHEN A.PAY_MOVE_DATE <= '2019-02-28' THEN A.PAY_MOVE_DATE END) AS FIRST_PAYMENT_M3,
				MAX(CASE WHEN A.PAY_MOVE_DATE <= '2019-02-28' THEN A.PAY_MOVE_DATE END) AS LAST_PAYMENT_M3,
				COUNT(CASE
				WHEN B.PAY_PM_TYPE_DESC = 'Account Money' THEN PAY_PAYMENT_ID
				END) AS PAYMENTS_ACC_MONEY,
				COUNT(CASE
				WHEN B.PAY_PM_TYPE_DESC = 'Credit Card' THEN PAY_PAYMENT_ID
				END) AS PAYMENTS_CREDIT_CARD,
				COUNT(CASE
				WHEN B.PAY_PM_TYPE_DESC = 'Debit Card' THEN PAY_PAYMENT_ID
				END) AS PAYMENTS_DEBIT_CARD,
				MAX(CASE
				WHEN TO_CHAR(A.PAY_MOVE_DATE, 'YYYYMM') = '201903' AND TPV_SEGMENT_DETAIL = 'Cellphone Recharge' THEN 1 ELSE 0
				END) AS TARGET
FROM		WHOWNER.BT_MP_PAY_PAYMENTS A
INNER JOIN WHOWNER.LK_MP_PAY_PAYMENT_METHODS B 
	ON B.PAY_PM_ID = A.PAY_PM_ID
WHERE		A.PAY_MOVE_DATE BETWEEN '2018-12-01' AND '2019-03-31'
				AND A.SIT_SITE_ID = 'MLA'
				AND A.TPV_FLAG = 1
				AND A.TPV_SEGMENT_ID = 'Wallet'
				AND A.PAY_CREATED_FROM IN ('1311377052931992','1945000207238192','2034912493936010','1505','7092')
GROUP BY 1
) WITH DATA UNIQUE PRIMARY INDEX (CUS_CUST_ID) ON COMMIT PRESERVE ROWS