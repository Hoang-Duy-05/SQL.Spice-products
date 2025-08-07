-- Mục tiêu: tìm những sản phẩm tìm năng để nhập về kinh doanh --

--1/ Sản phẩm có tổng số lượng bán và doanh thu cao nhất từng thương hiệu
with rank_Total_revenue as (Select 
Brand,
[Tên sản phẩm],
[Tổng doanh số],
[Tổng lượt bán],
[Giá bán],
dense_rank() over(partition by brand order by [Tổng doanh số] desc) as rank_total
from dhfood)

select *
from rank_Total_revenue
where rank_total <= 3

--2/ Sản phẩm có số lượng bán và doanh thu cao nhất từng thương hiệu trong 365 ngày gần nhất
with rank_365_last as (Select 
Brand,
[Tên sản phẩm],
[Doanh số 365 ngày gần nhất],
[Sản phẩm đã bán trong khoảng 365 ngày gần nhất],
[Giá bán],
dense_rank() over(partition by brand order by [Doanh số 365 ngày gần nhất] desc) as rank_365
from dhfood)

select *
from rank_365_last
where rank_365 <= 3
--Nhận xét: Trong 365 ngày qua, các sản phẩm có mức giá từ 25.000 đến 35.000 VND ghi nhận số lượng bán cao. 
--Tuy nhiên, xu hướng tiêu dùng đang cho thấy sự sụt giảm trong việc lựa chọn các sản phẩm cao cấp có giá từ 100.000 đến 500.000 VND, 
--Được phản ánh qua doanh thu giảm của một số thương hiệu sở hữu các sản phẩm giá cao.


--3/ Sản phẩm có tổng số lượng bán và doanh thu cao nhất từng thương hiệu so với sản phẩm trong 365 ngày gần nhất
with rank_Total_revenue as (Select 
Brand,
[Tên sản phẩm],
[Tổng doanh số],
[Tổng lượt bán],
[Giá bán],
dense_rank() over(partition by brand order by [Tổng doanh số] desc) as rank_total
from dhfood),

T_R as (select *
from rank_Total_revenue
where rank_total <= 3),

rank_365_last as (Select 
Brand,
[Tên sản phẩm],
[Doanh số 365 ngày gần nhất],
[Sản phẩm đã bán trong khoảng 365 ngày gần nhất],
[Giá bán],
dense_rank() over(partition by brand order by [Doanh số 365 ngày gần nhất] desc) as rank_365
from dhfood),

T_R_365 as (select *
from rank_365_last
where rank_365 <= 3)

select 
T_R.brand,
T_R.[Tên sản phẩm] as Tên_sản_phẩm_total,
T_R_365.[Tên sản phẩm] as Tên_sản_phẩm_365,
T_R.[Giá bán]
from T_R
full join T_R_365
on T_R.rank_total = T_R_365.rank_365 and T_R.brand = T_R_365.brand
order by brand, [Giá bán] desc

--4/ Các sản phẩm lọt vào top 5 doanh thu cả trong tổng số và trong 365 ngày gần nhất.

with rank_Total_revenue as (Select 
Brand,
[Tên sản phẩm],
[Tổng doanh số],
[Tổng lượt bán],
[Giá bán],
dense_rank() over(partition by brand order by [Tổng doanh số] desc) as rank_total
from dhfood),

T_R as (select *
from rank_Total_revenue
where rank_total <= 5),

rank_365_last as (Select 
Brand,
[Tên sản phẩm],
[Doanh số 365 ngày gần nhất],
[Sản phẩm đã bán trong khoảng 365 ngày gần nhất],
[Giá bán],
dense_rank() over(partition by brand order by [Doanh số 365 ngày gần nhất] desc) as rank_365
from dhfood),

T_R_365 as (select *
from rank_365_last
where rank_365 <= 5)

select 
T_R.brand,
T_R.[Tên sản phẩm] as Tên_sản_phẩm_total,
T_R_365.[Tên sản phẩm] as Tên_sản_phẩm_365,
T_R.[Giá bán]
from T_R
full join T_R_365
on T_R.rank_total = T_R_365.rank_365 and T_R.brand = T_R_365.brand
Where T_R.[Tên sản phẩm] = T_R_365.[Tên sản phẩm]
order by brand, [Giá bán] desc

--Nhận xét: Qua phân tích và so sánh, khoảng 7 sản phẩm liên tục duy trì vị trí trong top doanh thu, cho thấy đây là những sản phẩm tiêu dùng phổ biến và có sức hút mạnh mẽ. 
--Đề xuất xem xét kỹ lưỡng các sản phẩm này để lựa chọn nhập về kinh doanh.

