import psycopg2
from flask import Flask, jsonify, request

app = Flask("Bank_rest_api_app")

#connection as global argument because it's small app with local server
conn = psycopg2.connect(host="localhost",
                        port=5432,
                        database="bank",
                        user="postgres",
                        password="tkt@200974467"
)


@app.route("/api/v1/customers/<int:customer_id>", methods=['GET'])
def get_customer(customer_id):
    print(f"called /customers/customer_id/{customer_id}")
    with conn:
        with conn.cursor() as cur:
            sql = f"select * from customers where id = %s"
            cur.execute(sql, (customer_id, ))
            result = cur.fetchone()
            if result:
                ret_data = {
                    'id': result[0],
                    'passport_num': result[1],
                    'name': result[2],
                    'address': result[3]
                }
                return jsonify(ret_data)
            else:
                # no data for this customer_id
                return app.response_class(status=404)


@app.route("/api/v1/customers/<int:customer_id>/accounts", methods=['GET'])
def get_customer_accounts(customer_id):
    print(f"called /customers/customer_id/{customer_id}/accounts")
    with conn:
        with conn.cursor() as cur:
            sql = f"select ah.account_id, a.account_num from customers c " \
                  f"join account_holder ah on ah.customer_id = c.id " \
                  f"join accounts a on ah.account_id = a.id " \
                  f"where c.id=%s"
            cur.execute(sql, (customer_id, ))
            results = cur.fetchall()
            if results:
                ret_data = {
                    'number of accounts': len(results),
                    'account_id': results
                }
                return jsonify(ret_data)
            else:
                # no data for this customer_id
                return app.response_class(status=404)


@app.route("/api/v1/customers", methods=['GET'])
def get_all_customers():
    # ?page_num=1&results_per_page=20
    # “FILTER”:
    # GET /api/v1/customers?city=Tel-Aviv&name_contains=Aynbinder&page_num=2&results_per_page=20
    print(f"called api/v1/customers")
    with conn:
        with conn.cursor() as cur:
            sql = f"select * from customers"
            cur.execute(sql)
            results = cur.fetchall()
            if results:
                ret_data = {
                    'total customers': len(results),
                    'customers': results
                }
                return jsonify(ret_data)
            else:
                return app.response_class(status=404)


@app.route("/api/v1/customers", methods=['POST'])
def create_new_customer():
    # details in request body
    new_data = request.form
    str_list = []
    for field in new_data:
        str_list.append(field)
    sql = f"insert into customers ({','.join(str_list)})" \
          f"values(%s, %s, %s);"
    with conn:
        with conn.cursor() as cur:
            cur.execute(sql, tuple(new_data.values()))
            print(f"created customer passport number {new_data['passport_num']}")
            if cur.rowcount == 1:
                updated_obj = "select * from customers"
                cur.execute(updated_obj, )
                results = cur.fetchall()
                print(results)
                return app.response_class(status=200)
    return app.response_class(status=500)


@app.route("/api/v1/customers/<int:customer_id>", methods=['PUT'])
def update_customer_data(customer_id):
    # details in request body
    new_data = request.form
    # request.form is kind of dictionary
    update_str_list = []
    for field in new_data:
        update_str_list.append(f"{field}=%s")
    sql = f"update customers set {','.join(update_str_list)} where id=%s"
    with conn:
        with conn.cursor() as cur:
            cur.execute(sql, tuple(new_data.values()) + tuple([customer_id]))
            print(f"update customer no. {customer_id}")
            if cur.rowcount == 1:
                updated_obj = "select * from customers"
                cur.execute(updated_obj, )
                results = cur.fetchall()
                print(results)
                return app.response_class(status=200)
    return app.response_class(status=500)


@app.route("/api/v1/customers/<int:customer_id>", methods=['DELETE'])
def delete_customer(customer_id):
    print(f'called delete customer id no. {customer_id}')
    with conn:
        with conn.cursor() as cur:
            sql = f"delete from customers where id=%s"
            cur.execute(sql, (customer_id, ))
            if cur.rowcount == 1:
                updated_obj = "select * from customers"
                cur.execute(updated_obj, )
                results = cur.fetchall()
                print(results)
                return app.response_class(status=200)
    return app.response_class(status=500)


if __name__ == '__main__':
    app.run(host="0.0.0.0", debug=True)