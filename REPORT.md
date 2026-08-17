# Báo cáo LAB 17 — Data Pipeline Engineering

**Họ tên:** Bùi Thế Huy  **Lớp:** Track2-Day17  **Ngày:** 17/8/2026

---

## 0 · Kết quả `make verify`

<details>
<summary>Dán nguyên output ba lần chạy vào đây</summary>

```
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  LAB 17 · make verify
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  run 1/3 … 20.0s
  run 2/3 … 23.1s
  run 3/3 … 22.9s

  BẢNG                  ỔN ĐỊNH          SỐ HÀNG     KỲ VỌNG   GHI CHÚ
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     ✓ ok              12,480      12,480   ✓
  gold_feature_daily    ✓ ok               9,100       9,100   ✓
  gold_doc_chunks       ✓ ok              31,200      31,200   ✓
  quarantine_tickets    ✓ ok                 312         312   ✓

  CHECKSUM từng lượt
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     8dd7c98653    8dd7c98653    8dd7c98653   ✓
  gold_feature_daily    3db448685c    3db448685c    3db448685c   ✓
  gold_doc_chunks       92d8e50131    92d8e50131    92d8e50131   ✓
  quarantine_tickets    ebb89036fb    ebb89036fb    ebb89036fb   ✓

  KIỂM TRA KHÁC
  ──────────────────────────────────────────────────────────────────────────
  dbt test                                    ✓ 11/11 pass
  silver_tickets.priority ∈ 1..4, không NULL  ✓ sạch
  quarantine_tickets đúng số bản ghi lỗi      ✓ 312 / 312
  gold_training_set: 1 hàng / 1 ticket        ✓ không lặp
  dashboard rows scanned                      ✓ 5,000,000 → 132,734 (37.7×, cần ≥ 10×)
    số file parquet                           ✓ 5,000 → 14
    kết quả truy vấn không đổi                ✓
  DAG: catchup / max_active_runs              ✓ False / 1

  TỔNG KẾT
  ──────────────────────────────────────────────────────────────────────────
  ✓  1 · gold_training_set idempotent & đúng số hàng
  ✓  2 · gold_feature_daily đủ hàng (dữ liệu về muộn)
  ✓  3 · contract + quarantine + dbt test
  ✓  4 · gold_doc_chunks vẫn ổn định (đối chứng)
  ──────────────────────────────────────────────────────────────────────────
  4/4 tiêu chí đạt
```

</details>

Tổng kết: **4/4 tiêu chí đạt**

---

## 1 · Kích thước bảng training tăng sau mỗi lần chạy

| | |
|---|---|
| **Triệu chứng** | Bảng `gold_training_set` bị nhân bản dữ liệu, số hàng tăng lên 38,750 sau mỗi lần chạy lại `make pipeline` dù dữ liệu nguồn không đổi. Thao tác Clear Task trên Airflow khiến số hàng tăng đột biến. |
| **Nguyên nhân** | Model `gold_training_set` được định nghĩa là `incremental` nhưng không khai báo `unique_key`. DBT mặc định sử dụng chiến lược `append` (chèn thêm). Nguồn CDC chứa các bản ghi cập nhật (`op='u'`) lọt qua điều kiện lọc thời gian nhiều lần, dẫn đến việc chèn trùng lặp ticket. Tham số DAG Airflow `catchup=True` làm kích hoạt lại các run lịch sử khi Clear Task khiến lỗi nhân bản bị kích hoạt nhiều lần. |
| **Cách khắc phục** | - `dbt/models/gold/gold_training_set.sql`: Thêm `unique_key = 'ticket_id'` vào khối `config()` để ép dbt chuyển sang chiến lược `MERGE`.<br>- `dags/ai_training_pipeline.py`: Đặt `catchup=False` và `max_active_runs=1`. |
| **Bằng chứng** | trước: 38,750 hàng · sau: 12,480 hàng · checksum 3 lượt: 8dd7c98653 |

---

## 2 · Bảng đặc trưng theo ngày thiếu hàng ở các ngày quá khứ

| | |
|---|---|
| **Triệu chứng** | Bảng `gold_feature_daily` bị hụt khoảng 5% số hàng (chỉ có 8,645/9,100 hàng), chủ yếu hụt ở các ngày cũ đã chạy xong. |
| **P99 độ trễ đo được** | **2.73 ngày** (tương đương ~65.4 giờ) |
| **Lookback đã chọn** | 3 ngày — vì P99 độ trễ tới kho của sự kiện là 2.73 ngày, window 3 ngày đảm bảo bắt được 99% dữ liệu đến muộn mà không làm tăng quá nhiều chi phí tính toán. |
| **Nguyên nhân** | Sự kiện xảy ra ở ngày quá khứ nhưng đến kho muộn (Data arrival delay). Mốc lọc incremental cũ `event_date > (select max(event_date) from target)` chỉ quét các ngày lớn hơn ngày lớn nhất đã ghi. Do đó, các sự kiện đến muộn có `event_date` nhỏ hơn `max(event_date)` hiện tại bị bỏ qua hoàn toàn. |
| **Cách khắc phục** | - `dbt/models/gold/gold_feature_daily.sql`: Thêm `unique_key = ['event_date', 'customer_id']` vào khối `config()` để ghi đè khi tính lại.<br>- Sửa điều kiện incremental thành: `where event_date >= (select max(event_date) - interval 3 day from {{ this }})`. |
| **Bằng chứng** | trước: 8,645 hàng · sau: 9,100 hàng |

Vì sao chọn P99 làm căn cứ thay vì `max`? Chi phí của mỗi lựa chọn là gì?

> - **Lý do chọn P99**: P99 (2.73 ngày) đại diện cho ngưỡng thời gian mà 99% dữ liệu muộn sẽ tới kho. Chọn P99 giúp cân bằng hoàn hảo giữa tính đầy đủ của dữ liệu và chi phí tính toán.
> - **Chi phí của `max`**: Nếu chọn `max` độ trễ (có thể lên tới vài tuần/tháng do bản ghi ngoại lệ), mỗi lượt chạy incremental sẽ phải quét và tính toán lại một lượng khổng lồ dữ liệu lịch sử ở **mọi** lượt chạy về sau, làm tăng đáng kể thời gian chạy pipeline, chi phí tính toán và tài nguyên bộ nhớ.
> - **Chi phí của P99**: 1% dữ liệu đến trễ hơn 3 ngày sẽ bị bỏ qua (có thể bù đắp bằng các pipeline reconciliation định kỳ hàng tháng).

---

## 3 · Kiểu dữ liệu cột priority thay đổi giữa chu kỳ

| | |
|---|---|
| **Triệu chứng** | Từ ngày 08-10, mô hình dự đoán kém hẳn. 6,606 hàng có `priority` bị NULL hoặc nằm ngoài khoảng 1..4. Bảng `quarantine_tickets` ban đầu thiếu 312 hàng. |
| **Nguyên nhân** | Team Backend thay đổi cách biểu diễn cột `priority` từ dạng số (`1..4`) sang dạng chuỗi (`urgent`, `high`, ...). Do `contract.enforced` đang để `false`, dbt không chặn lỗi schema evolution này. Hàm `try_cast` ép nhãn chữ thành NULL nhưng lại cho phép các số không hợp lệ (`0`, `5`, `-1`) đi qua. |
| **Ba nhóm giá trị `priority` và cách xử lý từng nhóm** | 1. **Số hợp lệ (`1`..`4`)**: Giữ nguyên.<br>2. **Nhãn chữ (`urgent`, `high`, `medium`, `low`)**: Map tương ứng về `1`, `2`, `3`, `4`.<br>3. **Không hợp lệ (`P1`, `unknown`, `0`, `5`, `-1`, NULL)**: Chuyển thành NULL để đẩy vào Quarantine. |
| **Cách khắc phục** | - `dbt/macros/normalize_priority.sql`: Dùng `CASE WHEN` xử lý chuẩn hoá đủ 3 nhóm.<br>- `dbt/models/silver/silver_tickets.sql`: Lọc bỏ bản ghi trả về NULL trước khi thực hiện `row_number()`.<br>- `dbt/models/silver/quarantine_tickets.sql`: Đẩy các bản ghi trả về NULL vào bảng quarantine.<br>- `dbt/models/silver/schema.yml`: Bật `contract: enforced: true` và mở test `accepted_values: [1, 2, 3, 4]`. |
| **Bằng chứng** | `quarantine_tickets` = 312 hàng · `dbt test` 11/11 pass |

Câu hỏi thiết kế: nên chặn ở tầng Bronze hay Silver? Vì sao **không** để
pipeline dừng khi gặp bản ghi lỗi?

> - **Nên chặn/xử lý ở tầng Silver**: Tầng Bronze đóng vai trò là "Data Lake / Raw Store" lưu giữ nguyên vẹn dữ liệu gốc từ nguồn để làm bằng chứng audit và phục vụ debugging. Nếu chặn từ Bronze, dữ liệu lỗi sẽ bị mất vĩnh viễn và không thể điều tra nguyên nhân.
> - **Vì sao không để pipeline dừng**: Trong vận hành thực tế, vài trăm bản ghi hỏng không được phép làm dừng toàn bộ DAG và ngăn cản hàng trăm nghìn sự kiện/ticket hợp lệ khác đến tay người dùng cuối. Việc định tuyến bản ghi hỏng vào bảng Quarantine giúp pipeline tiếp tục vận hành thông suốt, đồng thời tạo hàng đợi cho đội vận hành xử lý sau.

---

## 4 · *(mở rộng, không bắt buộc)* Bài trong EXTRA.md

| | |
|---|---|
| **Bài đã làm** | Cả hai bài A và B |
| **Nguyên nhân** | **Bài A**: Lỗi small-file (5,000 file Parquet nhỏ) và đường dẫn lưu trữ không được partition theo ngày làm engine DuckDB phải mở và đọc toàn bộ file. Filter bọc bởi hàm `strftime` ngăn engine dùng thống kê min/max.<br>**Bài B**: Consumer cũ commit offset trước khi ghi dữ liệu (At-most-once), khiến khi bị crash giữa batch, dữ liệu lô đó chưa được ghi nhưng offset đã tăng -> Mất dữ liệu. |
| **Cách khắc phục** | **Bài A**: Sửa `tools/compact.py` thực hiện gộp file và partition theo `event_date`, cài đặt `row_group_size=100000`. Sửa `queries/dashboard.sql` trỏ tới `data/gold_events_v2/**/*.parquet` với `hive_partitioning=true` và filter theo `event_date`.<br>**Bài B**: Chuyển thứ tự trong `consume()` thành ghi dữ liệu trước, commit offset sau (At-least-once). Đặt `primary key (event_id)` trong DDL và dùng `INSERT INTO ... ON CONFLICT (event_id) DO UPDATE SET ...` trong `write_batch()` để đảm bảo tính Idempotent. |
| **Bằng chứng** | **Bài A**: Rows scanned giảm từ 5,000,000 xuống 132,734 (giảm 37.7×), số file parquet giảm từ 5,000 xuống 14 file, kết quả truy vấn không đổi.<br>**Bài B**: `make crash-test` đạt thành công (NHIỆM VỤ MỞ RỘNG B: ĐẠT). |

---

## 5 · Tổng kết

| Nhiệm vụ | Khi tiếp nhận một hệ thống chưa quen, tôi sẽ kiểm tra điều này trước tiên |
|---|---|
| 1 | Kiểm tra các model Incremental đã được khai báo `unique_key` và chiến lược ghi (`merge`/`delete+insert`) phù hợp với grain của bảng hay chưa, đồng thời kiểm tra tham số DAG Airflow (`catchup`, `max_active_runs`). |
| 2 | Đánh giá độ trễ dữ liệu đến kho (Data arrival delay / P99 latency) và kiểm tra mốc lọc của Incremental model để đảm bảo có Lookback Window phù hợp. |
| 3 | Kiểm tra trạng thái Data Contract (`enforced: true`), miền giá trị test (`accepted_values`), và cơ chế xử lý dữ liệu lỗi (Quarantine pattern) để tránh làm sập pipeline hoặc lọt dữ liệu rác. |
