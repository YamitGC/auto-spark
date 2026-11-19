import zipfile
import xml.etree.ElementTree as ET
import csv
import json
import os
import re
import sys

def col_letter_to_index(col_letter):
    """Convierte letras de columna Excel (A, B, AA, etc.) a índice 0-based."""
    index = 0
    for char in col_letter:
        index = index * 26 + (ord(char.upper()) - ord('A') + 1)
    return index - 1

def parse_sheet_rows(z, sheet_xml_path):
    tree = ET.fromstring(z.read(sheet_xml_path))
    rows_data = []
    
    for row in tree.findall('.//{http://schemas.openxmlformats.org/spreadsheetml/2006/main}row'):
        row_cells = {}
        max_col_idx = -1
        for c in row.findall('{http://schemas.openxmlformats.org/spreadsheetml/2006/main}c'):
            r_attr = c.attrib.get('r', '')
            match = re.match(r'([A-Z]+)(\d+)', r_attr)
            if not match:
                continue
            col_letter, _ = match.groups()
            col_idx = col_letter_to_index(col_letter)
            max_col_idx = max(max_col_idx, col_idx)
            
            t = c.attrib.get('t')
            # Extraer valor
            if t == 'inlineStr':
                is_node = c.find('{http://schemas.openxmlformats.org/spreadsheetml/2006/main}is')
                if is_node is not None:
                    t_node = is_node.find('{http://schemas.openxmlformats.org/spreadsheetml/2006/main}t')
                    val = t_node.text if t_node is not None and t_node.text is not None else ''
                else:
                    val = ''
            else:
                v_node = c.find('{http://schemas.openxmlformats.org/spreadsheetml/2006/main}v')
                val = v_node.text if v_node is not None and v_node.text is not None else ''
            
            row_cells[col_idx] = val
        
        # Construir fila completa rellenando columnas vacías
        row_list = [row_cells.get(i, '') for i in range(max_col_idx + 1)]
        rows_data.append(row_list)
        
    return rows_data

def main():
    # Permite pasar la ruta del Excel como parámetro en la terminal:
    # Ejemplo: python convert_xlsx_to_csv_jsonl.py mi_archivo.xlsx
    if len(sys.argv) > 1:
        xlsx_path = sys.argv[1]
    else:
        xlsx_path = "taller_data_engineer.xlsx"

    if not os.path.exists(xlsx_path):
        print(f"ERROR: No se encontró el archivo '{xlsx_path}'")
        print("Uso: python convert_xlsx_to_csv_jsonl.py [ruta_al_archivo.xlsx]")
        sys.exit(1)

    base_dir = "datos"
    inc_dir = os.path.join(base_dir, "incremental")
    os.makedirs(inc_dir, exist_ok=True)
    
    print(f"Abriendo {xlsx_path} y extrayendo las 6 hojas oficiales...")
    with zipfile.ZipFile(xlsx_path, 'r') as z:
        # Sheet 1: clientes -> datos/customers.csv
        rows_cust = parse_sheet_rows(z, 'xl/worksheets/sheet1.xml')
        with open(os.path.join(base_dir, "customers.csv"), "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerows(rows_cust)
        print(f" -> customers.csv: {len(rows_cust)-1} registros.")

        # Sheet 2: productos -> datos/products.csv
        rows_prod = parse_sheet_rows(z, 'xl/worksheets/sheet2.xml')
        with open(os.path.join(base_dir, "products.csv"), "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerows(rows_prod)
        print(f" -> products.csv: {len(rows_prod)-1} registros.")

        # Sheet 3: pedidos -> datos/orders.csv
        rows_ord = parse_sheet_rows(z, 'xl/worksheets/sheet3.xml')
        with open(os.path.join(base_dir, "orders.csv"), "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerows(rows_ord)
        print(f" -> orders.csv: {len(rows_ord)-1} registros.")

        # Sheet 4: detalle_pedidos -> datos/order_items.csv
        rows_items = parse_sheet_rows(z, 'xl/worksheets/sheet4.xml')
        with open(os.path.join(base_dir, "order_items.csv"), "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerows(rows_items)
        print(f" -> order_items.csv: {len(rows_items)-1} registros.")

        # Sheet 5: pedidos_incrementales -> datos/incremental/orders_incremental.jsonl
        rows_inc_ord = parse_sheet_rows(z, 'xl/worksheets/sheet5.xml')
        header_inc_ord = rows_inc_ord[0]
        with open(os.path.join(inc_dir, "orders_incremental.jsonl"), "w", encoding="utf-8") as f:
            for r in rows_inc_ord[1:]:
                obj = {header_inc_ord[i]: r[i] if i < len(r) else '' for i in range(len(header_inc_ord))}
                f.write(json.dumps(obj) + "\n")
        print(f" -> orders_incremental.jsonl: {len(rows_inc_ord)-1} registros.")

        # Sheet 6: detalle_incremental -> datos/incremental/order_items_incremental.jsonl
        rows_inc_items = parse_sheet_rows(z, 'xl/worksheets/sheet6.xml')
        header_inc_items = rows_inc_items[0]
        with open(os.path.join(inc_dir, "order_items_incremental.jsonl"), "w", encoding="utf-8") as f:
            for r in rows_inc_items[1:]:
                obj = {header_inc_items[i]: r[i] if i < len(r) else '' for i in range(len(header_inc_items))}
                f.write(json.dumps(obj) + "\n")
        print(f" -> order_items_incremental.jsonl: {len(rows_inc_items)-1} registros.")

    print("Extracción completada con éxito desde el Excel oficial.")

if __name__ == "__main__":
    main()
