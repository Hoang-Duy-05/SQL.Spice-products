-- 🎯 Mục tiêu:
-- Tìm các sản phẩm tiềm năng để nhập về kinh doanh, dựa trên doanh số tổng và trong 365 ngày gần nhất.

-- 1️⃣ Top 3 sản phẩm có tổng doanh số cao nhất từng thương hiệu
WITH rank_Total_revenue AS (
    SELECT 
        Brand,
        [Tên sản phẩm],
        [Tổng doanh số],
        [Tổng lượt bán],
        [Giá bán],
        DENSE_RANK() OVER (PARTITION BY Brand ORDER BY [Tổng doanh số] DESC) AS rank_total
    FROM dhfood
)
SELECT *
FROM rank_Total_revenue
WHERE rank_total <= 3;


-- 2️⃣ Top 3 sản phẩm doanh số cao nhất từng thương hiệu trong 365 ngày gần nhất
WITH rank_365_last AS (
    SELECT 
        Brand,
        [Tên sản phẩm],
        [Doanh số 365 ngày gần nhất],
        [Sản phẩm đã bán trong khoảng 365 ngày gần nhất],
        [Giá bán],
        DENSE_RANK() OVER (PARTITION BY Brand ORDER BY [Doanh số 365 ngày gần nhất] DESC) AS rank_365
    FROM dhfood
)
SELECT *
FROM rank_365_last
WHERE rank_365 <= 3;

-- Nhận xét:
-- - Các sản phẩm giá từ 25.000 - 35.000 VND bán chạy nhất.
-- - Các sản phẩm giá cao (100.000 - 500.000 VND) đang có xu hướng giảm doanh số.


-- 3️⃣ So sánh sản phẩm bán chạy tổng thể và trong 365 ngày gần nhất
WITH rank_Total_revenue AS (
    SELECT 
        Brand,
        [Tên sản phẩm],
        [Tổng doanh số],
        [Tổng lượt bán],
        [Giá bán],
        DENSE_RANK() OVER (PARTITION BY Brand ORDER BY [Tổng doanh số] DESC) AS rank_total
    FROM dhfood
),
T_R AS (
    SELECT * FROM rank_Total_revenue WHERE rank_total <= 3
),
rank_365_last AS (
    SELECT 
        Brand,
        [Tên sản phẩm],
        [Doanh số 365 ngày gần nhất],
        [Sản phẩm đã bán trong khoảng 365 ngày gần nhất],
        [Giá bán],
        DENSE_RANK() OVER (PARTITION BY Brand ORDER BY [Doanh số 365 ngày gần nhất] DESC) AS rank_365
    FROM dhfood
),
T_R_365 AS (
    SELECT * FROM rank_365_last WHERE rank_365 <= 3
)
SELECT 
    T_R.Brand,
    T_R.[Tên sản phẩm] AS Tên_sản_phẩm_total,
    T_R_365.[Tên sản phẩm] AS Tên_sản_phẩm_365,
    T_R.[Giá bán]
FROM T_R
FULL JOIN T_R_365
    ON T_R.rank_total = T_R_365.rank_365
    AND T_R.Brand = T_R_365.Brand
ORDER BY Brand, [Giá bán] DESC;


-- 4️⃣ Các sản phẩm lọt top 5 doanh thu ở cả tổng thể và trong 365 ngày gần nhất
WITH rank_Total_revenue AS (
    SELECT 
        Brand,
        [Tên sản phẩm],
        [Tổng doanh số],
        [Tổng lượt bán],
        [Giá bán],
        DENSE_RANK() OVER (PARTITION BY Brand ORDER BY [Tổng doanh số] DESC) AS rank_total
    FROM dhfood
),
T_R AS (
    SELECT * FROM rank_Total_revenue WHERE rank_total <= 5
),
rank_365_last AS (
    SELECT 
        Brand,
        [Tên sản phẩm],
        [Doanh số 365 ngày gần nhất],
        [Sản phẩm đã bán trong khoảng 365 ngày gần nhất],
        [Giá bán],
        DENSE_RANK() OVER (PARTITION BY Brand ORDER BY [Doanh số 365 ngày gần nhất] DESC) AS rank_365
    FROM dhfood
),
T_R_365 AS (
    SELECT * FROM rank_365_last WHERE rank_365 <= 5
)
SELECT 
    T_R.Brand,
    T_R.[Tên sản phẩm] AS Tên_sản_phẩm_total,
    T_R_365.[Tên sản phẩm] AS Tên_sản_phẩm_365,
    T_R.[Giá bán]
FROM T_R
FULL JOIN T_R_365
    ON T_R.rank_total = T_R_365.rank_365
    AND T_R.Brand = T_R_365.Brand
WHERE T_R.[Tên sản phẩm] = T_R_365.[Tên sản phẩm]
ORDER BY Brand, [Giá bán] DESC;

-- Nhận xét:
-- - Có khoảng 7 sản phẩm luôn nằm trong top doanh thu, chứng tỏ nhu cầu ổn định và tiềm năng cao.
-- - Đây là nhóm sản phẩm nên tập trung đánh giá kỹ trước khi quyết định nhập hang.
