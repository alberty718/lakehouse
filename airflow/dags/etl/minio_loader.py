import json

from minio import Minio
import trino


class MinioIcebergLoader:

    def __init__(self):

        self.minio = Minio(
            "minio:9000",
            access_key="minioadmin",
            secret_key="minioadmin",
            secure=False
        )

        self.conn = trino.dbapi.connect(
            host="trino",
            port=8080,
            user="airflow",
            catalog="iceberg",
            schema="silver"
        )

        self.cursor = self.conn.cursor()

    def read_folder(self, prefix):

        rows = []

        objects = self.minio.list_objects(
            "lakehouse",
            prefix=prefix,
            recursive=True
        )

        for obj in objects:

            if not obj.object_name.endswith(".json"):
                continue

            file = self.minio.get_object(
                "lakehouse",
                obj.object_name
            )

            data = json.loads(
                file.read().decode("utf-8")
            )

            file.close()
            file.release_conn()

            rows.extend(data)

        return rows

    def format_value(self, value, col):

        if value is None:
            return "NULL"

        if col in ["registration_date", "ingestion_batch_date"]:
            return f"DATE '{value}'"

        if col == "ts":

            value = str(value)

            if "+" in value:
                value = value.split("+")[0]

            value = value.replace("Z", "")
            value = value.replace("T", " ")

            return f"TIMESTAMP '{value}'"

        if isinstance(value, (int, float)):
            return str(value)

        value = str(value).replace("'", "''")

        return f"'{value}'"

    def insert(self, table, rows, columns):

        if not rows:
            return

        batch_size = 500

        for i in range(0, len(rows), batch_size):

            batch = rows[i:i + batch_size]

            values = []

            for row in batch:

                vals = [
                    self.format_value(
                        row.get(col),
                        col
                    )
                    for col in columns
                ]

                values.append(
                    "(" + ",".join(vals) + ")"
                )

            sql = f"""
            INSERT INTO {table} (
                {",".join(columns)}
            )
            VALUES
            {",".join(values)}
            """

            print(sql)

            self.cursor.execute(sql)

    def load_customers(self):

        rows = self.read_folder("raw/crm/")

        self.insert(
            "iceberg.silver.customers",
            rows,
            [
                "customer_id",
                "first_name",
                "last_name",
                "email",
                "city",
                "segment",
                "registration_date",
                "ingestion_batch_date"
            ]
        )

    def load_products(self):

        rows = self.read_folder("raw/products/")

        self.insert(
            "iceberg.silver.products",
            rows,
            [
                "product_id",
                "name",
                "category",
                "brand",
                "price",
                "currency"
            ]
        )

    def load_transactions(self):

        rows = self.read_folder("raw/pos/")

        transactions = []
        transaction_items = []

        for row in rows:

            transactions.append({
                "transaction_id": row["transaction_id"],
                "ts": row["timestamp"],
                "store_id": row["store_id"],
                "customer_id": row["customer_id"],
                "total_amount": row["total_amount"],
                "payment_method": row["payment_method"]
            })

            for item in row["items"]:
                transaction_items.append({
                    "transaction_id": row["transaction_id"],
                    "product_id": item["product_id"],
                    "quantity": item["quantity"],
                    "unit_price": item["unit_price"]
                })

        self.insert(
            "iceberg.silver.pos_transactions",
            transactions,
            [
                "transaction_id",
                "ts",
                "store_id",
                "customer_id",
                "total_amount",
                "payment_method"
            ]
        )

        self.insert(
            "iceberg.silver.transaction_items",
            transaction_items,
            [
                "transaction_id",
                "product_id",
                "quantity",
                "unit_price"
            ]
        )

    def close(self):
        self.conn.close()