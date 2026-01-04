from flask import Flask, render_template, request, redirect, url_for, flash, jsonify, send_file
import mysql.connector
import pandas as pd
import os
from dotenv import load_dotenv
from datetime import datetime
import math
from io import BytesIO
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders

# Thử import APScheduler, nếu chưa cài đặt thì bỏ qua tính năng tự động
try:
    from apscheduler.schedulers.background import BackgroundScheduler
    HAS_SCHEDULER = True
except ImportError:
    HAS_SCHEDULER = False
    print("⚠️ Cảnh báo: Chưa cài đặt 'apscheduler'. Tính năng gửi email tự động sẽ không hoạt động.")

# 1. Tải biến môi trường
load_dotenv()

app = Flask(__name__)
app.secret_key = 'supersecretkey'  # Cần thiết cho flash messages

# 2. Hàm kết nối Database
def get_db_connection():
    try:
        conn = mysql.connector.connect(
            host=os.getenv("DB_HOST"),
            port=int(os.getenv("DB_PORT") or 3306),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            database=os.getenv("DB_NAME"),
            ssl_ca=os.getenv("DB_SSL_CA"),
            ssl_disabled=False
        )
        return conn
    except mysql.connector.Error as e:
        print(f"❌ Lỗi kết nối MySQL: {e}")
        return None

# --- ROUTES ---

@app.route('/')
def index():
    return redirect(url_for('supplier'))

# === SUPPLIER ===
@app.route('/supplier', methods=['GET', 'POST'])
def supplier():
    conn = get_db_connection()
    if not conn:
        flash("Lỗi kết nối Database", "danger")
        return render_template('supplier.html', suppliers=[])

    cursor = conn.cursor(dictionary=True)

    if request.method == 'POST':
        # Xử lý thêm mới
        mancc = request.form.get('mancc')
        tenncc = request.form.get('tenncc')
        qg = request.form.get('qg')
        
        if not tenncc:
            flash("Vui lòng nhập tên nhà cung cấp", "warning")
        else:
            try:
                cursor.execute("INSERT INTO nhacungcap (MANCC, TENNCC, QG) VALUES (%s, %s, %s)", (mancc, tenncc, qg))
                conn.commit()
                flash("Thêm mới thành công!", "success")
            except Exception as e:
                flash(f"Lỗi: {e}", "danger")

    # Lấy danh sách
    page = request.args.get('page', 1, type=int)
    per_page = 10
    offset = (page - 1) * per_page

    suppliers = []
    total_pages = 1
    try:
        search = request.args.get('q', '')
        if search:
            cursor.execute("SELECT COUNT(*) as total FROM nhacungcap WHERE MANCC LIKE %s OR TENNCC LIKE %s", (f"%{search}%", f"%{search}%"))
            res = cursor.fetchone()
            total_records = res['total'] if res else 0
            cursor.fetchall() # Clear buffer
            cursor.execute("SELECT * FROM nhacungcap WHERE MANCC LIKE %s OR TENNCC LIKE %s LIMIT %s OFFSET %s", (f"%{search}%", f"%{search}%", per_page, offset))
        else:
            cursor.execute("SELECT COUNT(*) as total FROM nhacungcap")
            res = cursor.fetchone()
            total_records = res['total'] if res else 0
            cursor.fetchall() # Clear buffer
            cursor.execute("SELECT * FROM nhacungcap LIMIT %s OFFSET %s", (per_page, offset))
        suppliers = cursor.fetchall()
        total_pages = math.ceil(total_records / per_page) if total_records > 0 else 1
    except Exception as e:
        flash(f"Lỗi tải dữ liệu: {e}", "danger")
    finally:
        cursor.close()
        conn.close()
    return render_template('supplier.html', suppliers=suppliers, page=page, total_pages=total_pages)

@app.route('/supplier/delete/<mancc>')
def delete_supplier(mancc):
    conn = get_db_connection()
    if conn:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM nhacungcap WHERE MANCC = %s", (mancc,))
        conn.commit()
        conn.close()
        flash(f"Đã xóa NCC: {mancc}", "success")
    return redirect(url_for('supplier'))

@app.route('/supplier/update', methods=['POST'])
def update_supplier():
    conn = get_db_connection()
    if conn:
        cursor = conn.cursor()
        mancc = request.form.get('mancc')
        tenncc = request.form.get('tenncc')
        qg = request.form.get('qg')
        try:
            cursor.execute("UPDATE nhacungcap SET TENNCC=%s, QG=%s WHERE MANCC=%s", (tenncc, qg, mancc))
            conn.commit()
            flash("Cập nhật thành công!", "success")
        except Exception as e:
            flash(f"Lỗi: {e}", "danger")
        conn.close()
    return redirect(url_for('supplier'))

# === MASTERDATA ===
@app.route('/masterdata', methods=['GET', 'POST'])
def masterdata():
    conn = get_db_connection()
    if not conn: return "DB Error"
    cursor = conn.cursor(dictionary=True)

    if request.method == 'POST':
        # Xử lý thêm mới
        mancc = request.form.get('mancc')
        sku = request.form.get('sku')
        desc = request.form.get('description')
        qty = request.form.get('quantity')
        weight = float(request.form.get('weight') or 0)
        length = float(request.form.get('length') or 0)
        width = float(request.form.get('width') or 0)
        height = float(request.form.get('height') or 0)
        cbm = (length * width * height) / 1000000
        refix = request.form.get('refix')
        loosecase = request.form.get('loosecase')
        kindpallet = request.form.get('kindpallet')

        try:
            sql = """INSERT INTO masterdata (MANCC, sku, description, quantity, weight, length, width, height, cbm, refix, loosecase, kindpallet) 
                     VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
            cursor.execute(sql, (mancc, sku, desc, qty, weight, length, width, height, cbm, refix, loosecase, kindpallet))
            conn.commit()
            flash("Thêm Master Data thành công!", "success")
        except Exception as e:
            flash(f"Lỗi: {e}", "danger")

    # Lấy danh sách + NCC dropdown
    page = request.args.get('page', 1, type=int)
    per_page = 10
    offset = (page - 1) * per_page

    # Sorting logic
    sort_by = request.args.get('sort_by', 'sku')
    order = request.args.get('order', 'asc')
    
    # Map tên tham số URL sang tên cột trong DB để tránh SQL Injection
    valid_columns = {'sku': 'm.sku', 'TENNCC': 'n.TENNCC', 'quantity': 'm.quantity', 'cbm': 'm.cbm'}
    sort_col = valid_columns.get(sort_by, 'm.sku')
    if order not in ['asc', 'desc']: order = 'asc'

    search = request.args.get('q', '')
    if search:
        count_sql = """SELECT COUNT(*) as total FROM masterdata m LEFT JOIN nhacungcap n ON m.MANCC = n.MANCC 
                       WHERE m.sku LIKE %s OR m.description LIKE %s"""
        cursor.execute(count_sql, (f"%{search}%", f"%{search}%"))
        total_records = cursor.fetchone()['total']

        sql = f"""SELECT m.*, n.TENNCC FROM masterdata m LEFT JOIN nhacungcap n ON m.MANCC = n.MANCC 
                 WHERE m.sku LIKE %s OR m.description LIKE %s ORDER BY {sort_col} {order.upper()} LIMIT %s OFFSET %s"""
        cursor.execute(sql, (f"%{search}%", f"%{search}%", per_page, offset))
    else:
        cursor.execute("SELECT COUNT(*) as total FROM masterdata")
        total_records = cursor.fetchone()['total']
        
        sql = f"""SELECT m.*, n.TENNCC FROM masterdata m LEFT JOIN nhacungcap n ON m.MANCC = n.MANCC 
                  ORDER BY {sort_col} {order.upper()} LIMIT %s OFFSET %s"""
        cursor.execute(sql, (per_page, offset))
    items = cursor.fetchall()
    total_pages = math.ceil(total_records / per_page)
    
    cursor.execute("SELECT MANCC, TENNCC FROM nhacungcap")
    suppliers = cursor.fetchall()
    
    conn.close()
    return render_template('masterdata.html', items=items, suppliers=suppliers, page=page, total_pages=total_pages, sort_by=sort_by, order=order)

@app.route('/masterdata/delete/<sku>')
def delete_masterdata(sku):
    conn = get_db_connection()
    if conn:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM masterdata WHERE sku = %s", (sku,))
        conn.commit()
        conn.close()
        flash(f"Đã xóa SKU: {sku}", "success")
    return redirect(url_for('masterdata'))

@app.route('/masterdata/update', methods=['POST'])
def update_masterdata():
    conn = get_db_connection()
    if conn:
        cursor = conn.cursor()
        sku = request.form.get('sku')
        mancc = request.form.get('mancc')
        desc = request.form.get('description')
        qty = request.form.get('quantity')
        weight = float(request.form.get('weight') or 0)
        length = float(request.form.get('length') or 0)
        width = float(request.form.get('width') or 0)
        height = float(request.form.get('height') or 0)
        cbm = (length * width * height) / 1000000
        refix = request.form.get('refix')
        loosecase = request.form.get('loosecase')
        kindpallet = request.form.get('kindpallet')
        cartonperpallet = request.form.get('cartonperpallet')

        try:
            sql = """UPDATE masterdata SET MANCC=%s, description=%s, quantity=%s, weight=%s, 
                     length=%s, width=%s, height=%s, cbm=%s, refix=%s, loosecase=%s, kindpallet=%s, cartonperpallet=%s 
                     WHERE sku=%s"""
            cursor.execute(sql, (mancc, desc, qty, weight, length, width, height, cbm, refix, loosecase, kindpallet, cartonperpallet, sku))
            conn.commit()
            flash("Cập nhật Master Data thành công!", "success")
        except Exception as e:
            flash(f"Lỗi: {e}", "danger")
        conn.close()
    return redirect(url_for('masterdata'))

# === BBR REPORT ===
@app.route('/bbr', methods=['GET', 'POST'])
def bbr():
    conn = get_db_connection()
    if not conn: return "DB Error"
    
    if request.method == 'POST':
        file = request.files['file']
        if file:
            try:
                # Xử lý logic CSV tùy chỉnh
                df_input = pd.read_csv(file)
                df_input.columns = df_input.columns.str.strip()
                
                cursor = conn.cursor()
                cursor.execute("SELECT sku, kindpallet FROM masterdata")
                master_dict = {str(row[0]): row[1] for row in cursor.fetchall()}
                
                cursor.execute("SELECT keycheck FROM bbrreport WHERE Status IS NULL")
                existing_keys = {str(row[0]) for row in cursor.fetchall() if row[0]}
                
                updates = []
                inserts = []
                
                for _, row in df_input.iterrows():
                    def get_str(col):
                        val = row.get(col)
                        return str(val).strip() if pd.notna(val) else ''

                    po = get_str('PO Number')
                    item = get_str('Item No')
                    parent_po = get_str('Parent PO')
                    origin = get_str('origin')
                    vndr = get_str('VNDR CD')
                    keycheck = f"{po}_{item}_{parent_po}"
                    
                    try:
                        d_dt = pd.to_datetime(row.get('DELIVERY DT'))
                        add_days = 3 if str(origin).upper() == 'VN' else 14
                        new_date = d_dt + pd.Timedelta(days=add_days)
                        new_date_str = new_date.strftime('%Y-%m-%d')
                        week_num = new_date.isocalendar()[1]
                    except:
                        new_date_str = None
                        week_num = None
                    
                    q_pck = pd.to_numeric(row.get('QTY per PCK'), errors='coerce') or 1
                    qty_val = (pd.to_numeric(row.get('QTY'), errors='coerce') or 0) / q_pck
                    
                    if keycheck in existing_keys:
                        updates.append((new_date_str, week_num, qty_val, keycheck))
                    else:
                        cbm = pd.to_numeric(row.get('MC CBM'), errors='coerce') or 0
                        total_cbm = qty_val * cbm
                        kind = master_dict.get(item, None)
                        inserts.append((keycheck, origin, po, item, vndr, parent_po, new_date_str, qty_val, cbm, week_num, kind, total_cbm))
                
                if updates:
                    cursor.executemany("UPDATE bbrreport SET deliverydate=%s, week=%s, qty=%s WHERE keycheck=%s AND Status IS NULL", updates)
                if inserts:
                    cursor.executemany("INSERT INTO bbrreport (keycheck, origin, PO, item, supplier, parentpo, deliverydate, qty, cbm, week, kindpallet, total_cbm) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", inserts)
                
                conn.commit()
                flash(f"Đã xử lý xong! Cập nhật: {len(updates)}, Thêm mới: {len(inserts)}", "success")
            except Exception as e:
                flash(f"Lỗi xử lý file: {e}", "danger")

    # Hiển thị dữ liệu
    query = """
        SELECT b.*, n.TENNCC 
        FROM bbrreport b 
        LEFT JOIN nhacungcap n ON b.supplier = n.MANCC
    """
    df = pd.read_sql(query, conn)
    conn.close()
    
    # Lấy danh sách tuần (Week) để hiển thị dropdown
    weeks = []
    if 'week' in df.columns:
        # Chuyển đổi dữ liệu cột week sang số để đảm bảo lọc chính xác
        df['week'] = pd.to_numeric(df['week'], errors='coerce')
        weeks = sorted(df['week'].dropna().unique().astype(int))

    # Lọc theo tuần nếu có tham số
    selected_week = request.args.get('week')
    if selected_week and selected_week.isdigit() and 'week' in df.columns:
        df = df[df['week'] == int(selected_week)]

    # Tìm kiếm trên DataFrame
    search = request.args.get('q', '')
    if search:
        mask = df.apply(lambda x: x.astype(str).str.contains(search, case=False, na=False)).any(axis=1)
        df = df[mask]
    
    # --- SORTING ---
    sort_by = request.args.get('sort_by')
    order = request.args.get('order', 'asc')
    
    if sort_by and sort_by in df.columns:
        ascending = True if order == 'asc' else False
        df = df.sort_values(by=sort_by, ascending=ascending)

    # Tính toán thống kê đơn giản
    total_cbm = df['total_cbm'].sum() if 'total_cbm' in df.columns else 0
    
    # Thống kê theo Parent PO
    po_stats = []
    if not df.empty and 'parentpo' in df.columns:
        # Tạo bản sao để đảm bảo dữ liệu số cho việc tính toán
        df_stats = df.copy()
        df_stats['total_cbm'] = pd.to_numeric(df_stats['total_cbm'], errors='coerce').fillna(0)
        df_stats['qty'] = pd.to_numeric(df_stats['qty'], errors='coerce').fillna(0)
        df_stats['TENNCC'] = df_stats['TENNCC'].fillna('')
        
        po_grouped = df_stats.groupby(['parentpo', 'TENNCC']).agg({
            'qty': 'sum',
            'total_cbm': 'sum'
        }).reset_index()
        
        po_grouped = po_grouped.sort_values(by='total_cbm', ascending=False)
        po_stats = po_grouped.to_dict(orient='records')

    # Thống kê theo kindpallet
    pallet_stats = {
        '1m2': 0,
        '1m6': 0,
        '1m9': 0,
        '1.5': 0
    }
    if 'kindpallet' in df.columns and 'total_cbm' in df.columns:
        stats_grouped = df.groupby('kindpallet')['total_cbm'].sum()
        cbm_1m2 = stats_grouped.get('1m2', 0)
        cbm_1m6 = stats_grouped.get('1m6', 0)
        cbm_1m9 = stats_grouped.get('1m9', 0)
        
        pallet_stats['1m2'] = (cbm_1m2 / 3.06) * 1.5 if cbm_1m2 > 0 else 0
        pallet_stats['1m6'] = (cbm_1m6 / 4.08) * 1.5 if cbm_1m6 > 0 else 0
        pallet_stats['1m9'] = (cbm_1m9 / 4.85) * 1.5 if cbm_1m9 > 0 else 0
        pallet_stats['1.5'] = stats_grouped.get('1.5', 0)

    # Thống kê Chipboard
    chipboard_stats = {
        '1210': 0,
        '1610': 0,
        '1910': 0
    }
    if 'item' in df.columns and 'total_cbm' in df.columns:
        # Tính tổng CBM cho các nhóm SKU Chipboard
        chipboard_stats['1210'] = df[df['item'].isin(["LLR68948", "LLR68952", "LLR68953"])]['total_cbm'].sum()
        chipboard_stats['1610'] = df[df['item'].isin(["LLR68947", "LLR68951"])]['total_cbm'].sum()
        chipboard_stats['1910'] = df[df['item'].isin(["LLR68946", "LLR68950", "LLR68960"])]['total_cbm'].sum()

    # Phân trang cho DataFrame
    page = request.args.get('page', 1, type=int)
    per_page = 50
    total_records = len(df)
    total_pages = math.ceil(total_records / per_page)
    start = (page - 1) * per_page
    end = start + per_page
    data_page = df.iloc[start:end].to_dict(orient='records')

    return render_template('bbr.html', data=data_page, total_cbm=total_cbm, page=page, total_pages=total_pages, weeks=weeks, selected_week=selected_week, sort_by=sort_by, order=order, pallet_stats=pallet_stats, po_stats=po_stats, chipboard_stats=chipboard_stats)

@app.route('/bbr/export_po_stats')
def export_po_stats():
    conn = get_db_connection()
    if not conn: return "DB Error"
    
    # 1. Lấy dữ liệu (Giống logic route bbr)
    query = """
        SELECT b.*, n.TENNCC 
        FROM bbrreport b 
        LEFT JOIN nhacungcap n ON b.supplier = n.MANCC
    """
    df = pd.read_sql(query, conn)
    conn.close()

    # 2. Áp dụng bộ lọc (Tuần & Tìm kiếm)
    if 'week' in df.columns:
        df['week'] = pd.to_numeric(df['week'], errors='coerce')
    
    selected_week = request.args.get('week')
    if selected_week and selected_week.isdigit() and 'week' in df.columns:
        df = df[df['week'] == int(selected_week)]

    search = request.args.get('q', '')
    if search:
        mask = df.apply(lambda x: x.astype(str).str.contains(search, case=False, na=False)).any(axis=1)
        df = df[mask]

    # 3. Gom nhóm dữ liệu
    if not df.empty and 'parentpo' in df.columns:
        df['total_cbm'] = pd.to_numeric(df['total_cbm'], errors='coerce').fillna(0)
        df['qty'] = pd.to_numeric(df['qty'], errors='coerce').fillna(0)
        df['TENNCC'] = df['TENNCC'].fillna('')
        
        po_grouped = df.groupby(['parentpo', 'TENNCC']).agg({
            'qty': 'sum',
            'total_cbm': 'sum'
        }).reset_index()
        
        po_grouped = po_grouped.sort_values(by='total_cbm', ascending=False)
        po_grouped.columns = ['Parent PO', 'Supplier', 'Tổng Số Kiện', 'Tổng CBM']
    else:
        po_grouped = pd.DataFrame(columns=['Parent PO', 'Supplier', 'Tổng Số Kiện', 'Tổng CBM'])

    # 4. Xuất ra Excel
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        po_grouped.to_excel(writer, index=False, sheet_name='PO Statistics')
    output.seek(0)
    
    return send_file(output, download_name="po_statistics.xlsx", as_attachment=True, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

@app.route('/bbr/delete_week', methods=['POST'])
def delete_bbr_week():
    conn = get_db_connection()
    if conn:
        week = request.form.get('week_to_delete')
        if week:
            try:
                cursor = conn.cursor()
                cursor.execute("DELETE FROM bbrreport WHERE week = %s", (week,))
                conn.commit()
                deleted_count = cursor.rowcount
                cursor.close()
                flash(f"Đã xóa {deleted_count} dòng dữ liệu của tuần {week}", "success")
            except Exception as e:
                flash(f"Lỗi khi xóa: {e}", "danger")
        conn.close()
    return redirect(url_for('bbr'))

# === INBOUND ===
@app.route('/inbound', methods=['GET', 'POST'])
def inbound():
    conn = get_db_connection()
    if not conn: return "DB Error"
    cursor = conn.cursor(dictionary=True)

    if request.method == 'POST':
        packing = request.form.get('packing')
        po = request.form.get('po')
        sku = request.form.get('sku')
        qty = float(request.form.get('qty') or 0)
        date = request.form.get('date')
        cont = request.form.get('container')
        labour = request.form.get('labour')
        
        # Lấy thông tin phụ
        cursor.execute("SELECT supplier, cbm FROM bbrreport WHERE item = %s LIMIT 1", (sku,))
        res = cursor.fetchone()
        mancc = res['supplier'] if res else ''
        unit_cbm = float(res['cbm']) if res and res['cbm'] else 0
        total_cbm = unit_cbm * qty
        
        try:
            sql = "INSERT INTO inbound (MANCC, po, sku, carton, contxe, datercv, cbm, labour, PackinglistNo) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
            cursor.execute(sql, (mancc, po, sku, qty, cont, date, total_cbm, labour, packing))
            conn.commit()
            flash("Thêm Inbound thành công!", "success")
        except Exception as e:
            flash(f"Lỗi: {e}", "danger")

    # --- TỐI ƯU HÓA TRUY VẤN SQL ---
    
    base_query = "FROM inbound i LEFT JOIN nhacungcap n ON i.MANCC = n.MANCC"
    conditions = []
    params = []

    search = request.args.get('q', '')
    if search:
        conditions.append("(i.po LIKE %s OR i.sku LIKE %s OR i.PackinglistNo LIKE %s)")
        params.extend([f"%{search}%"] * 3)

    from_date = request.args.get('from_date')
    if from_date:
        conditions.append("i.datercv >= %s")
        params.append(from_date)

    to_date = request.args.get('to_date')
    if to_date:
        conditions.append("i.datercv <= %s")
        params.append(to_date)

    where_clause = " WHERE " + " AND ".join(conditions) if conditions else ""

    # 1. Lấy danh sách Inbound chi tiết (Có alias cho Edit Modal)
    inbounds_sql = f"""
        SELECT 
            i.id, i.PackinglistNo, i.PackinglistNo as packing,
            i.po, i.sku, n.TENNCC as supplier, 
            i.carton, i.carton as qty,
            i.cbm, i.contxe, i.contxe as container,
            i.datercv, i.datercv as date, i.labour
        {base_query}
        {where_clause}
        ORDER BY i.datercv DESC
    """
    cursor.execute(inbounds_sql, params)
    inbounds = cursor.fetchall()
    
    # Lấy danh sách PO cho dropdown
    cursor.execute("SELECT DISTINCT parentpo FROM bbrreport WHERE parentpo IS NOT NULL")
    pos = [row['parentpo'] for row in cursor.fetchall()]
    
    # Lấy danh sách Container cho autocomplete
    cursor.execute("SELECT DISTINCT contxe FROM inbound WHERE contxe IS NOT NULL AND contxe != '' ORDER BY contxe DESC")
    containers = [row['contxe'] for row in cursor.fetchall()]
    
    # 2. Thống kê Packing List (Tính toán trực tiếp bằng SQL thay vì Pandas)
    stats_sql = f"""
        SELECT 
            i.PackinglistNo as `Packing List`,
            MAX(i.datercv) as `Ngày nhập hàng`,
            COALESCE(SUM(i.cbm), 0) as `Tổng CBM`,
            COALESCE(SUM(i.carton), 0) as `Tổng Số Kiện`,
            MAX(n.TENNCC) as `Nhà Cung Cấp`
        {base_query}
        {where_clause}
        GROUP BY i.PackinglistNo
        ORDER BY `Ngày nhập hàng` DESC
    """
    cursor.execute(stats_sql, params)
    stats = cursor.fetchall()

    # Phân trang cho Stats
    page = request.args.get('page', 1, type=int)
    per_page = 10
    total_records = len(stats)
    total_pages = math.ceil(total_records / per_page)
    start = (page - 1) * per_page
    end = start + per_page
    stats_page = stats[start:end]

    conn.close()
    return render_template('inbound.html', inbounds=inbounds, pos=pos, stats=stats_page, page=page, total_pages=total_pages, containers=containers, today=datetime.now().strftime('%Y-%m-%d'))

# API lấy SKU theo PO (cho Inbound form)
@app.route('/api/get_skus/<po>')
def get_skus(po):
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    # Lấy thêm thông tin số lượng (qty)
    cursor.execute("SELECT item, SUM(qty) as qty FROM bbrreport WHERE parentpo = %s GROUP BY item ORDER BY item", (po,))
    skus = cursor.fetchall()
    conn.close()
    # Chuyển đổi Decimal sang float nếu cần để tránh lỗi JSON
    for s in skus:
        s['qty'] = float(s['qty']) if s['qty'] is not None else 0
    return jsonify(skus)

# API lấy thông tin chi tiết SKU (Supplier, CBM)
@app.route('/api/get_sku_info/<sku>')
def get_sku_info(sku):
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    
    cursor.execute("SELECT supplier, cbm FROM bbrreport WHERE item = %s LIMIT 1", (sku,))
    res = cursor.fetchone()
    
    data = {'supplier': '', 'cbm': 0}
    if res:
        data['cbm'] = float(res['cbm']) if res['cbm'] else 0
        supplier_code = res['supplier']
        
        cursor.execute("SELECT TENNCC FROM nhacungcap WHERE MANCC = %s", (supplier_code,))
        supp_res = cursor.fetchone()
        data['supplier'] = supp_res['TENNCC'] if supp_res else supplier_code
        
    conn.close()
    return jsonify(data)

# API lấy tổng số lượng đã nhập của PO
@app.route('/api/get_po_imported/<po>')
def get_po_imported(po):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT SUM(carton) FROM inbound WHERE po = %s", (po,))
    res = cursor.fetchone()
    total = float(res[0]) if res and res[0] else 0
    conn.close()
    return jsonify({'total_imported': total})

# Route In Packing List
@app.route('/inbound/print/<packinglist_no>')
def print_packinglist(packinglist_no):
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT i.*, n.TENNCC 
        FROM inbound i 
        LEFT JOIN nhacungcap n ON i.MANCC = n.MANCC 
        WHERE i.PackinglistNo = %s
    """, (packinglist_no,))
    items = cursor.fetchall()
    conn.close()
    
    if not items:
        return "Không tìm thấy Packing List"
        
    total_qty = sum(item['carton'] for item in items)
    total_cbm = sum(item['cbm'] for item in items)
    date_rcv = items[0]['datercv']
    supplier = items[0]['TENNCC'] if items[0]['TENNCC'] else items[0]['MANCC']
    contxe = items[0]['contxe']
    
    return render_template('print_packinglist.html', pl=packinglist_no, items=items, total_qty=total_qty, total_cbm=total_cbm, date=date_rcv, supplier=supplier, contxe=contxe)

@app.route('/inbound/delete/<int:id>', methods=['POST'])
def delete_inbound(id):
    conn = get_db_connection()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM inbound WHERE id = %s", (id,))
            conn.commit()
            flash("Đã xóa bản ghi Inbound thành công!", "success")
        except Exception as e:
            flash(f"Lỗi khi xóa: {e}", "danger")
        finally:
            if conn.is_connected():
                conn.close()
    return redirect(url_for('inbound'))

@app.route('/inbound/update', methods=['POST'])
def update_inbound():
    conn = get_db_connection()
    if conn:
        cursor = conn.cursor(dictionary=True)
        id = request.form.get('id')
        packing = request.form.get('packing')
        po = request.form.get('po')
        sku = request.form.get('sku')
        qty = float(request.form.get('qty') or 0)
        date = request.form.get('date')
        cont = request.form.get('container')
        labour = request.form.get('labour')
        
        # Tính lại CBM
        cursor.execute("SELECT cbm FROM bbrreport WHERE item = %s LIMIT 1", (sku,))
        res = cursor.fetchone()
        unit_cbm = float(res['cbm']) if res and res['cbm'] else 0
        total_cbm = unit_cbm * qty
        
        try:
            sql = "UPDATE inbound SET PackinglistNo=%s, po=%s, sku=%s, carton=%s, contxe=%s, datercv=%s, cbm=%s, labour=%s WHERE id=%s"
            cursor.execute(sql, (packing, po, sku, qty, cont, date, total_cbm, labour, id))
            conn.commit()
            flash("Cập nhật Inbound thành công!", "success")
        except Exception as e:
            flash(f"Lỗi cập nhật: {e}", "danger")
        conn.close()
    return redirect(url_for('inbound'))

def generate_outsource_data():
    """Hàm hỗ trợ tạo dữ liệu báo cáo Outsource (dùng chung cho Export và Email)"""
    conn = get_db_connection()
    if not conn: return None, None
    
    # Tính toán khoảng thời gian: 21 tháng trước đến 20 tháng này
    today = datetime.now()
    current_month = today.month
    current_year = today.year
    
    # Ngày kết thúc: 20 của tháng hiện tại
    end_date = datetime(current_year, current_month, 20)
    
    # Ngày bắt đầu: 21 của tháng trước
    if current_month == 1:
        start_month = 12
        start_year = current_year - 1
    else:
        start_month = current_month - 1
        start_year = current_year
        
    start_date = datetime(start_year, start_month, 21)
    
    start_str = start_date.strftime('%Y-%m-%d')
    end_str = end_date.strftime('%Y-%m-%d')
    
    query = """
        SELECT 
            i.datercv as `Ngày nhập`,
            i.contxe as `Cont/Xe`, 
            SUM(i.carton) as `Tổng Số Carton`, 
            SUM(i.cbm) as `Tổng CBM`
        FROM inbound i 
        WHERE i.labour = 'Outsource' 
        AND i.datercv >= %s AND i.datercv <= %s
        GROUP BY i.datercv, i.contxe
        ORDER BY i.datercv ASC
    """
    
    df = pd.read_sql(query, conn, params=(start_str, end_str))
    conn.close()
    
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Outsource Report')
    output.seek(0)
    
    filename = f"Outsource_Report_{start_str}_{end_str}.xlsx"
    return output, filename

@app.route('/inbound/export_outsource_report')
def export_outsource_report():
    output, filename = generate_outsource_data()
    if not output:
        return "Lỗi kết nối Database hoặc không có dữ liệu"
        
    return send_file(output, download_name=filename, as_attachment=True, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

def send_outsource_email_task():
    """Tác vụ gửi email tự động"""
    # Chỉ chạy vào ngày 20 hàng tháng
    if datetime.now().day != 20:
        return

    print("📧 Đang bắt đầu tác vụ gửi email báo cáo Outsource...")
    excel_file, filename = generate_outsource_data()
    if not excel_file:
        print("❌ Không thể tạo file báo cáo.")
        return

    sender = os.getenv("MAIL_USERNAME")
    recipients = os.getenv("MAIL_RECIPIENTS", "").split(',')
    password = os.getenv("MAIL_PASSWORD")
    smtp_server = os.getenv("MAIL_SERVER", "smtp.gmail.com")
    smtp_port = int(os.getenv("MAIL_PORT") or 587)

    if not (sender and recipients and password):
        print("❌ Thiếu cấu hình Email trong .env")
        return

    msg = MIMEMultipart()
    msg['From'] = sender
    msg['To'] = ", ".join(recipients)
    msg['Subject'] = f"Báo cáo Outsource Định Kỳ - {filename}"
    
    body = f"Kính gửi,\n\nĐính kèm là báo cáo Outsource từ ngày 21 tháng trước đến ngày 20 tháng này.\n\nTrân trọng,\nWMS System"
    msg.attach(MIMEText(body, 'plain'))
    
    part = MIMEBase('application', 'octet-stream')
    part.set_payload(excel_file.read())
    encoders.encode_base64(part)
    part.add_header('Content-Disposition', f'attachment; filename= {filename}')
    msg.attach(part)
    
    try:
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()
        server.login(sender, password)
        server.sendmail(sender, recipients, msg.as_string())
        server.quit()
        print(f"✅ Đã gửi email báo cáo thành công đến: {recipients}")
    except Exception as e:
        print(f"❌ Lỗi gửi email: {e}")

# === OUTBOUND ===
@app.route('/outbound', methods=['GET', 'POST'])
def outbound():
    conn = get_db_connection()
    if not conn: return "DB Error"
    cursor = conn.cursor(dictionary=True)

    if request.method == 'POST':
        if 'file' in request.files:
            file = request.files['file']
            # Lấy thông tin Job No và Ngày từ form nhập liệu
            manual_do_no = request.form.get('do_no')
            manual_date = request.form.get('date')
            manual_container = request.form.get('container')
            is_add_more = request.form.get('add_more')

            if file.filename != '':
                try:
                    # Đọc file Excel hoặc CSV
                    if file.filename.endswith('.csv'):
                        df = pd.read_csv(file, dtype=str)
                    else:
                        df = pd.read_excel(file, dtype=str)
                    
                    df = df.fillna('')
                    
                    # Chuẩn hóa tên cột
                    df.columns = df.columns.str.strip()
                    
                    # Lấy dữ liệu CBM để tính toán nhanh
                    cursor.execute("SELECT sku, cbm, loosecase, kindpallet FROM masterdata")
                    master_data = {
                        row['sku']: {
                            'cbm': float(row['cbm']) if row['cbm'] else 0,
                            'loosecase': row['loosecase'],
                            'kindpallet': row['kindpallet']
                        } 
                        for row in cursor.fetchall()
                    }
                    
                    cursor.execute("SELECT item, cbm FROM bbrreport WHERE cbm IS NOT NULL")
                    bbr_cbm = {row['item']: float(row['cbm']) for row in cursor.fetchall() if row['cbm']}
                    
                    inserts = []
                    for _, row in df.iterrows():
                        # Hàm lấy giá trị linh hoạt theo nhiều tên cột
                        def get_col(names):
                            for name in names:
                                if name in df.columns:
                                    return row[name]
                            return None

                        # Áp dụng Job No và Ngày từ form cho tất cả các dòng
                        do_no = manual_do_no
                        date_out = manual_date if manual_date else datetime.now().strftime('%d-%m-%Y')
                        container = manual_container
                        remark = 'add_more' if is_add_more else ''
                        po = get_col(['PPO', 'PO Number', 'po'])
                        sku = get_col(['SKU', 'Item', 'Mã hàng', 'sku'])
                        childpo= get_col(['Child PO', 'ChildPO', 'childpo'])
                        fdc = str(childpo)[:3] if childpo else ''
                        qty = pd.to_numeric(get_col(['Sum of Carton', 'Quantity', 'Số lượng', 'Carton', 'carton']), errors='coerce') or 0
                       
                        
                        if sku and qty > 0:
                            # Tính CBM
                            info = master_data.get(sku)
                            unit_cbm = 0
                            loose_carton = ''
                            kind_pallet = ''

                            if info:
                                unit_cbm = info['cbm']
                                loose_carton = info['loosecase']
                                kind_pallet = info['kindpallet']
                            else:
                                unit_cbm = bbr_cbm.get(sku, 0)
                                
                            total_cbm = unit_cbm * qty
                            
                            inserts.append((do_no, po, sku, qty, date_out, total_cbm,  childpo, fdc, remark, loose_carton, kind_pallet, container))
                    if inserts:
                        sql = "INSERT INTO outbound (jobno, po, sku, carton, datercv, cbm, childpo, fdc, remark, loosecarton, kindpallet, container) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                        cursor.executemany(sql, inserts)
                        conn.commit()
                        flash(f"Đã import thành công {len(inserts)} dòng dữ liệu!", "success")
                    else:
                        flash("Không tìm thấy dữ liệu hợp lệ trong file.", "warning")
                except Exception as e:
                    flash(f"Lỗi khi import file: {e}", "danger")
        else:
            flash("Vui lòng chọn file để upload", "warning")

    # --- LỌC VÀ HIỂN THỊ ---
    conditions = []
    params = []
    
    search = request.args.get('q', '')
    if search:
        conditions.append("(jobno LIKE %s OR parentpo LIKE %s OR sku LIKE %s)")
        params.extend([f"%{search}%"] * 3)
        
    from_date = request.args.get('from_date')
    if from_date:
        conditions.append("datercv >= %s")
        params.append(from_date)

    to_date = request.args.get('to_date')
    if to_date:
        conditions.append("datercv <= %s")
        params.append(to_date)
        
    where_clause = " WHERE " + " AND ".join(conditions) if conditions else ""
    
    # Lấy danh sách Outbound
    sql = f"SELECT * FROM outbound {where_clause} ORDER BY datercv DESC"
    cursor.execute(sql, params)
    outbounds = cursor.fetchall()
    
    # Thống kê theo DO
    stats_sql = f"""
            SELECT 
                jobno as `DO Number`,
                container ,
                seal ,
                datestuff,
                MAX(datercv) as `Ngày nhận picking hàng`,
                COALESCE(SUM(cbm), 0) as `Tổng CBM`,
                COALESCE(SUM(carton), 0) as `Tổng Số Kiện`
                
            FROM outbound
            {where_clause}
            GROUP BY jobno, container,seal,datestuff
            ORDER BY `Ngày nhận picking hàng` DESC
        
        """
    cursor.execute(stats_sql, params)
    stats = cursor.fetchall()
    
    # Dropdowns
    cursor.execute("SELECT DISTINCT parentpo FROM bbrreport WHERE parentpo IS NOT NULL")
    pos = [row['parentpo'] for row in cursor.fetchall()]
    
    cursor.execute("SELECT DISTINCT container FROM outbound WHERE container IS NOT NULL ORDER BY container DESC")
    containers = [row['container'] for row in cursor.fetchall()]

    # Phân trang
    page = request.args.get('page', 1, type=int)
    per_page = 10
    total_records = len(outbounds)
    total_pages = math.ceil(total_records / per_page)
    start = (page - 1) * per_page
    end = start + per_page
    outbounds_page = outbounds[start:end]
    
    conn.close()
    return render_template('outbound.html', outbounds=outbounds_page, pos=pos, stats=stats, page=page, total_pages=total_pages, containers=containers, today=datetime.now().strftime('%Y-%m-%d'))

@app.route('/outbound/delete/<int:id>', methods=['POST'])
def delete_outbound(id):
    conn = get_db_connection()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM outbound WHERE id = %s", (id,))
            conn.commit()
            flash("Đã xóa bản ghi Outbound thành công!", "success")
        except Exception as e:
            flash(f"Lỗi khi xóa: {e}", "danger")
        conn.close()
    return redirect(url_for('outbound'))

@app.route('/outbound/update', methods=['POST'])
def update_outbound():
    conn = get_db_connection()
    if conn:
        cursor = conn.cursor(dictionary=True)
        id = request.form.get('id')
        do_no = request.form.get('do_no')
        po = request.form.get('po')
        sku = request.form.get('sku')
        qty = float(request.form.get('qty') or 0)
        date = request.form.get('date')
        cont = request.form.get('container')
        loosecarton = request.form.get('loosecarton')
        kindpallet = request.form.get('kindpallet')
        
        
        # Tính lại CBM
        cursor.execute("SELECT cbm FROM masterdata WHERE sku = %s LIMIT 1", (sku,))
        res = cursor.fetchone()
        if not res:
            cursor.execute("SELECT cbm FROM bbrreport WHERE item = %s LIMIT 1", (sku,))
            res = cursor.fetchone()
        unit_cbm = float(res['cbm']) if res and res['cbm'] else 0
        total_cbm = unit_cbm * qty
        
        try:
            sql = "UPDATE outbound SET jobno=%s, po=%s, sku=%s, carton=%s, datercv=%s, cbm=%s, loosecarton=%s, kindpallet=%s, container=%s WHERE id=%s"
            
        except Exception as e:
            flash(f"Lỗi cập nhật: {e}", "danger")
        conn.close()
    return redirect(url_for('outbound'))

@app.route('/outbound/update_info', methods=['POST'])
def update_outbound_info():
    conn = get_db_connection()
    if conn:
        cursor = conn.cursor()
        do_no = request.form.get('do_no')
        cont = request.form.get('container')
        seal = request.form.get('seal')
        date = request.form.get('date')
        
        updates = []
        params = []
        
        if cont:
            updates.append("container = %s")
            params.append(cont)
        if seal:
            updates.append("seal = %s")
            params.append(seal)
        if date:
            updates.append("datercv = %s")
            params.append(date)
            
        if updates and do_no:
            try:
                # Cập nhật cho tất cả các dòng thuộc Job No này
                sql = f"UPDATE outbound SET {', '.join(updates)} WHERE do_no = %s"
                params.append(do_no)
                cursor.execute(sql, tuple(params))
                conn.commit()
                flash(f"Đã cập nhật thông tin cho Job No: {do_no}", "success")
            except Exception as e:
                flash(f"Lỗi cập nhật: {e}", "danger")
        else:
            flash("Vui lòng nhập Job No và ít nhất một thông tin cần cập nhật", "warning")
            
        conn.close()
    return redirect(url_for('outbound'))

@app.route('/outbound/print/<path:do_no>')
def print_deliverynote(do_no):
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    # Lấy thông tin chi tiết của Job No
    cursor.execute("SELECT * FROM outbound WHERE jobno = %s", (do_no,))
    items = cursor.fetchall()
    conn.close()
    
    if not items:
        return "Không tìm thấy Delivery Note cho Job No này"
        
    total_qty = sum(item['carton'] for item in items)
    total_cbm = sum(item['cbm'] for item in items)
    date_out = items[0]['datercv']
    container = items[0].get('container') or items[0].get('contxe') or ''
    seal = items[0].get('seal', '')
    customer = items[0].get('customer', '')
    
    return render_template('print_deliverynote.html', do_no=do_no, items=items, total_qty=total_qty, total_cbm=total_cbm, date=date_out, container=container, seal=seal, customer=customer)

@app.route('/outbound/print_pickinglist/<path:do_no>')
def print_pickinglist(do_no):
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    
    # Lấy dữ liệu và sắp xếp theo FDC, PO, SKU
    cursor.execute("SELECT * FROM outbound WHERE jobno = %s ORDER BY fdc, parentpo, sku", (do_no,))
    items = cursor.fetchall()
    conn.close()
    
    if not items:
        return "Không tìm thấy Picking List cho Job No này"

    # Gom nhóm theo FDC
    grouped_data = {}
    for item in items:
        fdc = item['fdc'] if item['fdc'] else 'Khác'
        if fdc not in grouped_data:
            grouped_data[fdc] = {
                'items': [],
                'total_cbm': 0,
                'total_carton': 0,
                'total_loose_carton': 0
            }
        
        grouped_data[fdc]['items'].append(item)
        
        cbm = float(item['cbm']) if item['cbm'] else 0
        carton = float(item['carton']) if item['carton'] else 0
        
        grouped_data[fdc]['total_cbm'] += cbm
        grouped_data[fdc]['total_carton'] += carton
        
        # Tính tổng Loose Carton (nếu cột loosecarton có dữ liệu)
        if item.get('looscarton') == 'Y':
            grouped_data[fdc]['total_loose_carton'] += carton

    grand_total_pallet_1m2 = 0
    grand_total_pallet_1m9 = 0

    # Tính toán số lượng Pallet dự kiến cho từng nhóm FDC
    for fdc, data in grouped_data.items():
        cbm = data['total_cbm']
        p1m2 = cbm / 3.06 if cbm else 0
        p1m9 = cbm / 4.85 if cbm else 0
        data['pallet_1m2'] = p1m2
        data['pallet_1m9'] = p1m9
        grand_total_pallet_1m2 += p1m2
        grand_total_pallet_1m9 += p1m9

    date_out = items[0]['datercv']
    container = items[0].get('container') or items[0].get('contxe') or ''
    
    return render_template('print_pickinglist.html', do_no=do_no, grouped_data=grouped_data, date=date_out, container=container, total_pallet_1m2=grand_total_pallet_1m2, total_pallet_1m9=grand_total_pallet_1m9)

@app.route('/scanfile', methods=['GET', 'POST'])
def scanfile():
    conn = get_db_connection()
    if not conn: return "DB Error"
    cursor = conn.cursor(dictionary=True)

    jobno = request.args.get('jobno', '')
    comparison_data = []
    summary = {'total_ordered': 0, 'total_scanned': 0, 'diff': 0}

    if request.method == 'POST':
        # Xử lý Upload File Scan
        jobno = request.form.get('jobno')
        files = request.files.getlist('file')
        is_replace = request.form.get('replace') # Checkbox để xóa dữ liệu cũ

        if files and jobno:
            try:
                if is_replace:
                    cursor.execute("DELETE FROM scanfile WHERE jobno = %s", (jobno,))
                
                # Lấy remark từ Master Data
                cursor.execute("SELECT sku, remark FROM masterdata")
                master_remarks = {row['sku']: str(row['remark']).strip() for row in cursor.fetchall() if row['remark']}

                total_inserted = 0
                file_details = []
                for file in files:
                    if file.filename == '': continue
                    
                    if file.filename.endswith('.csv'):
                        df = pd.read_csv(file, dtype=str)
                        df = pd.read_csv(file, header=None, dtype=str)
                    else:
                        df = pd.read_excel(file, dtype=str)
                    
                    # Chuẩn hóa cột
                    df.columns = df.columns.str.strip().str.lower()
                    
                    # Tìm cột SKU và Qty
                    sku_col = next((c for c in df.columns if c in ['sku', 'item', 'barcode', 'mã hàng']), None)
                    qty_col = next((c for c in df.columns if c in ['qty', 'quantity', 'số lượng', 'sl']), None)
                    df = pd.read_excel(file, header=None, dtype=str)

                    if sku_col:
                        inserts = []
                        for _, row in df.iterrows():
                            sku = str(row[sku_col]).strip()
                            qty = float(row[qty_col]) if qty_col and pd.notna(row[qty_col]) else 1.0
                            if sku:
                                inserts.append((jobno, sku, qty))
                    inserts = []
                    for _, row in df.iterrows():
                        # Bỏ qua dòng nếu không đủ cột (H là cột thứ 8)
                        if len(row) < 14: continue

                        # Map cột theo index (A=0, B=1, ...)
                        # release_key=cột B (1)
                        # sscc=cột C (2)
                        # master_delivery=cột D (3)
                        # qty= cột E (4)
                        # master_ctl= cột F (5)
                        # master_st_company= cột G (6)
                        # master_add1=cột H (7)
                        
                        release_key = str(row[1]).strip() if pd.notna(row[1]) else ''
                        sscc = str(row[2]).strip() if pd.notna(row[2]) else ''
                        master_delivery = str(row[3]).strip() if pd.notna(row[3]) else ''
                        
                        try:
                            qty = float(row[4]) if pd.notna(row[4]) else 0
                        except:
                            qty = 0
                            
                        master_ctl = str(row[5]).strip() if pd.notna(row[5]) else ''
                        master_st_company = str(row[6]).strip() if pd.notna(row[6]) else ''
                        master_add1 = str(row[7]).strip() if pd.notna(row[7]) else ''
                        master_add2 = str(row[8]).strip() if pd.notna(row[8]) else ''
                        master_add3 = str(row[9]).strip() if pd.notna(row[9]) else ''
                        master_add4 = str(row[10]).strip() if pd.notna(row[10]) else ''
                        ship_to = str(row[11]).strip() if pd.notna(row[11]) else ''
                        st_zip = str(row[12]).strip() if pd.notna(row[12]) else ''
                        barcode = str(row[13]).strip() if pd.notna(row[13]) else ''
                        sku = str(row[14]).strip() if pd.notna(row[14]) else '' 
                        jobno = jobno.strip()
                        master_val = master_remarks.get(sku, '')
                        tag_label = 'Y' if sku == master_val else 'N'
                        jobno_type = f"{jobno}_{master_delivery[:3]}"
                        pallet = ''
                        pallet_type = ''
                        jobscan = ''
                        time_scan = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        # Kiểm tra dữ liệu cơ bản (ví dụ phải có SSCC hoặc Release Key)
                        if sscc or release_key:
                            inserts.append((jobno, release_key, sscc, master_delivery, qty, master_ctl, master_st_company, master_add1, master_add2, master_add3, master_add4, ship_to, st_zip, barcode, sku, tag_label, jobno_type, pallet, pallet_type, time_scan,jobscan))
                    if inserts:
                        sql = "INSERT INTO scanfile (jobno, release_key, sscc, master_delivery, qty, master_ctl, master_st_company, master_add1,master_add2,master_add3,master_add4,ship_to,st_zip,barcode,sku,tag_label,jobno_type, pallet, pallet_type, time_scan,jobscan) VALUES (%s, %s, %s, %s, %s, %s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, %s, %s, %s,%s)"
                        cursor.executemany(sql, inserts)
                        total_inserted += len(inserts)
                        file_details.append(f"{file.filename} ({len(inserts)} dòng)")
                    else:
                        file_details.append(f"{file.filename} (0 dòng)")
                
                conn.commit()
                if total_inserted > 0:
                    flash(f"Đã import tổng cộng {total_inserted} dòng scan cho Job No: {jobno}. Chi tiết: {', '.join(file_details)}", "success")
                else:
                    flash("Không tìm thấy dữ liệu hợp lệ trong các file.", "warning")
            except Exception as e:
                flash(f"Lỗi xử lý file: {e}", "danger")
            
            return redirect(url_for('scanfile', jobno=jobno))

    # Lấy dữ liệu so sánh nếu có Job No
    if jobno:
        # Lấy tổng Outbound (Ordered)
        cursor.execute("SELECT sku, SUM(carton) as ordered_qty FROM outbound WHERE jobno = %s GROUP BY sku", (jobno,))
        outbound_data = {row['sku']: float(row['ordered_qty']) for row in cursor.fetchall()}

        # Lấy tổng Scan
        cursor.execute("SELECT sku, COUNT(sscc) as scanned_qty, SUM(CASE WHEN tag_label = 'N' THEN 1 ELSE 0 END) as error_labels, MAX(tag_label) as tag_label FROM scanfile WHERE jobno = %s GROUP BY sku", (jobno,))
        scan_data = {row['sku']: {'qty': float(row['scanned_qty']), 'error_labels': int(row['error_labels']), 'tag_label': row['tag_label']} for row in cursor.fetchall()}

        # Gộp danh sách SKU
        all_skus = set(outbound_data.keys()) | set(scan_data.keys())

        for sku in all_skus:
            ordered = outbound_data.get(sku, 0)
            scan_info = scan_data.get(sku, {'qty': 0, 'error_labels': 0, 'tag_label': ''})
            scanned = scan_info['qty']
            error_labels = scan_info['error_labels']
            tag_label = scan_info['tag_label']
            diff = scanned - ordered
            comparison_data.append({'sku': sku, 'ordered': ordered, 'scanned': scanned, 'diff': diff, 'tag_error': error_labels, 'tag_label': tag_label})
            
            summary['total_ordered'] += ordered
            summary['total_scanned'] += scanned

        summary['diff'] = summary['total_scanned'] - summary['total_ordered']

    conn.close()
    return render_template('import_scanfile.html', jobno=jobno, data=comparison_data, summary=summary)

@app.route('/scanfile/delete', methods=['POST'])
def delete_scan_job():
    conn = get_db_connection()
    if conn:
        jobno = request.form.get('jobno')
        if jobno:
            try:
                cursor = conn.cursor()
                cursor.execute("DELETE FROM scanfile WHERE jobno = %s", (jobno,))
                conn.commit()
                flash(f"Đã xóa toàn bộ dữ liệu scan của Job No: {jobno}", "success")
            except Exception as e:
                flash(f"Lỗi khi xóa: {e}", "danger")
        conn.close()
    return redirect(url_for('scanfile'))

@app.route('/api/scan_details')
def api_scan_details():
    jobno = request.args.get('jobno')
    sku = request.args.get('sku')
    
    conn = get_db_connection()
    if not conn: return jsonify([])
    
    cursor = conn.cursor(dictionary=True)
    query = """
        SELECT release_key, COUNT(sscc) as sscc_count 
        FROM scanfile 
        WHERE jobno = %s AND sku = %s 
        GROUP BY release_key
        ORDER BY release_key
    """
    cursor.execute(query, (jobno, sku))
    data = cursor.fetchall()
    conn.close()
    
    return jsonify(data)

# === PALLET MANAGEMENT ===
@app.route('/pallet', methods=['GET', 'POST'])
def pallet():
    conn = get_db_connection()
    if not conn: return "DB Error"
    cursor = conn.cursor(dictionary=True)

    if request.method == 'POST':
        date = request.form.get('date')
        pallet_type = request.form.get('pallet_type')
        action = request.form.get('action')
        qty = int(request.form.get('quantity') or 0)
        remark = request.form.get('remark')

        if qty > 0:
            try:
                cursor.execute("INSERT INTO pallet_management (date, pallet_type, action, quantity, remark) VALUES (%s, %s, %s, %s, %s)", (date, pallet_type, action, qty, remark))
                conn.commit()
                flash("Đã lưu giao dịch pallet thành công!", "success")
            except Exception as e:
                flash(f"Lỗi: {e}", "danger")
        else:
            flash("Số lượng phải lớn hơn 0", "warning")

    # 1. Tính tồn kho (ALL TIME - Luôn tính trên toàn bộ dữ liệu để hiển thị đúng tồn kho hiện tại)
    cursor.execute("SELECT pallet_type, action, SUM(quantity) as total FROM pallet_management GROUP BY pallet_type, action")
    summary_rows = cursor.fetchall()
    
    summary = {
        '1m2': {'in': 0, 'out': 0, 'stock': 0},
        '1m6': {'in': 0, 'out': 0, 'stock': 0},
        '1m9': {'in': 0, 'out': 0, 'stock': 0}
    }
    
    for row in summary_rows:
        p_type = row['pallet_type']
        qty = float(row['total'] or 0)
        if p_type in summary:
            if row['action'] == 'IN':
                summary[p_type]['in'] += qty
                summary[p_type]['stock'] += qty
            elif row['action'] == 'OUT':
                summary[p_type]['out'] += qty
                summary[p_type]['stock'] -= qty

    # 2. Lấy dữ liệu lịch sử (Có lọc theo ngày để hiển thị bảng)
    from_date = request.args.get('from_date')
    to_date = request.args.get('to_date')
    
    query = "SELECT * FROM pallet_management"
    params = []
    conditions = []
    
    if from_date:
        conditions.append("date >= %s")
        params.append(from_date)
    if to_date:
        conditions.append("date <= %s")
        params.append(to_date)
        
    if conditions:
        query += " WHERE " + " AND ".join(conditions)
        
    query += " ORDER BY date DESC, id DESC"
    
    cursor.execute(query, tuple(params))
    history = cursor.fetchall()

    conn.close()
    return render_template('pallet.html', history=history, summary=summary, today=datetime.now().strftime('%Y-%m-%d'), safety_threshold=50)

@app.route('/pallet/export')
def export_pallet():
    conn = get_db_connection()
    if not conn: return "DB Error"
    
    from_date = request.args.get('from_date')
    to_date = request.args.get('to_date')
    
    query = "SELECT date, pallet_type, action, quantity, remark FROM pallet_management"
    params = []
    conditions = []
    
    if from_date:
        conditions.append("date >= %s")
        params.append(from_date)
    if to_date:
        conditions.append("date <= %s")
        params.append(to_date)
        
    if conditions:
        query += " WHERE " + " AND ".join(conditions)
        
    query += " ORDER BY date DESC, id DESC"
    
    df = pd.read_sql(query, conn, params=params)
    conn.close()
    
    # Đổi tên cột cho đẹp
    df.columns = ['Ngày', 'Loại Pallet', 'Hành động', 'Số lượng', 'Ghi chú']
    
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Lịch sử Pallet')
    output.seek(0)
    
    filename = f"Pallet_History_{from_date if from_date else 'All'}_{to_date if to_date else 'All'}.xlsx"
    return send_file(output, download_name=filename, as_attachment=True, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

@app.route('/pallet/delete/<int:id>', methods=['POST'])
def delete_pallet(id):
    conn = get_db_connection()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM pallet_management WHERE id = %s", (id,))
            conn.commit()
            flash("Đã xóa giao dịch pallet!", "success")
        except Exception as e:
            flash(f"Lỗi xóa: {e}", "danger")
        conn.close()
    return redirect(url_for('pallet'))

if __name__ == "__main__":
    if HAS_SCHEDULER:
        scheduler = BackgroundScheduler()
        # Kiểm tra mỗi ngày vào lúc 8:00 sáng
        scheduler.add_job(func=send_outsource_email_task, trigger="cron", hour=8)
        scheduler.start()
        print("⏰ Đã khởi động Scheduler gửi báo cáo tự động.")
        
    app.run(debug=True)
