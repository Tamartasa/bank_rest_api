import datetime
import random

import psycopg2
from flask import Flask, jsonify, request

app = Flask("Bank_rest_api_app")

# connection as global argument because it's small app with local server
conn = psycopg2.connect(host="localhost",
                        port=5432,
                        database="bank",
                        user="postgres",
                        password="tkt@200974467"
                        )

# ------------ Customers -----------------------------------------------------------

@app.route("/api/v1/customers/<int:customer_id>", methods=['GET'])
def get_customer(customer_id):
    """
        The function returns details about customer by customer_id
        """

    print(f"called /customers/customer_id/{customer_id}")
    with conn:
        with conn.cursor() as cur:
            sql = f"select * from customers where id = %s"
            cur.execute(sql, (customer_id,))
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
    """
        The function returns all customer's accounts
        """
    print(f"called /customers/customer_id/{customer_id}/accounts")
    with conn:
        with conn.cursor() as cur:
            sql = f"select ah.account_id, a.account_num from customers c " \
                  f"join account_holder ah on ah.customer_id = c.id " \
                  f"join accounts a on ah.account_id = a.id " \
                  f"where c.id=%s"
            cur.execute(sql, (customer_id,))
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
    """
        The function get query params and returns all customers (with filter if passed)
        params: page_num, results_per_page, passport_num, name_customer, address
        """
    # ?page_num=1&results_per_page=20
    # “FILTER”:
    # GET /api/v1/customers?address=Tel-Aviv&name_contains=Aynbinder&page_num=2&results_per_page=20

    print(f"called api/v1/customers")

    page_num = request.args.get('page_num')
    results_per_page = request.args.get('results_per_page')
    default_num_page = 20
    update_str_list = []
    values_list = []
    for arg in request.args:
        if arg != 'page_num' and arg != 'results_per_page':
            update_str_list.append(f"{arg} ilike %s")
            values = request.args.get(arg)
            values_list.append(f"%{values}%")
    with conn:
        with conn.cursor() as cur:
            if len(update_str_list) == 0:
                sql = f"select * from customers"
            if len(update_str_list) > 0:
                sql = f"select * from customers where {(' and '.join(update_str_list))}"
            cur.execute(sql, tuple(values_list))
            if results_per_page:
                results = cur.fetchmany(int(results_per_page))
            else:
                results = cur.fetchmany(default_num_page)
            if results:
                ret_data = {
                    'total customers in this page': len(results),
                    'customers': []}
                for r in results:
                    ret_data_r = {'id': r[0],
                                  'passport_num': r[1],
                                  'name': r[2],
                                  'address': r[3]
                }
                    ret_data['customers'].append(ret_data_r)
                return jsonify(ret_data)
            else:
                return app.response_class(status=404)


@app.route("/api/v1/customers", methods=['POST'])
def create_new_customer():
    """
      The function create new customer
      params: passport_num -> int, customer_name -> str with '' address -> str with ''
      """
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
    """
        the function updtes cutomer's details. gets customer_id and query params
        """
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
            cur.execute(sql, (customer_id,))
            if cur.rowcount == 1:
                updated_obj = "select * from customers"
                cur.execute(updated_obj, )
                results = cur.fetchall()
                print(results)
                return app.response_class(status=200)
    return app.response_class(status=500)

# ------------ Accounts -----------------------------------------------------------

@app.route("/api/v1/accounts/<int:account_id>", methods=['GET'])
def get_account(account_id):
    """
      The function returns account's details by account_id
      """
    print(f"called /accounts/account_id/{account_id}")
    with conn:
        with conn.cursor() as cur:
            sql = f"select * from accounts where id = %s"
            cur.execute(sql, (account_id,))
            result = cur.fetchone()
            if result:
                ret_data = {
                    'id': result[0],
                    'account_num': result[1],
                    'max_limit': result[2],
                    'balance': result[3]
                }
                return jsonify(ret_data)
            else:
                # no data for this customer_id
                return app.response_class(status=404)


@app.route("/api/v1/accounts", methods=['GET'])
def get_all_accounts():
    # ?page_num=1&results_per_page=20          <==========================
    # “FILTER”:
    print(f"called api/v1/accounts")
    with conn:
        with conn.cursor() as cur:
            if len(request.args) == 0:
                # no query params
                sql = f"select * from accounts"
            if len(request.args) > 0:
                update_str_list = []
                for arg in request.args:
                    update_str_list.append(f"{arg}=%s")
                sql = f"select * from accounts where {('and '.join(update_str_list))}"
            cur.execute(sql, tuple(request.args.values()))
            results = cur.fetchall()
            if results:
                ret_data = {
                    'total accounts': len(results),
                    'accounts': []}
                for r in results:
                    ret_data_r = {'id': r[0],
                                  'account_num': r[1],
                                  'max_limit': r[2],
                                  'balance': r[3]
                }
                    ret_data['accounts'].append(ret_data_r)
                return jsonify(ret_data)
            else:
                return app.response_class(status=404)


@app.route("/api/v1/accounts", methods=['POST'])
def create_new_account():
    """
       The function create new account
       params: customer_id -> int: The customer for whom you want to open an account,
               max_limit -> int, balance -> int
       """
    # given customer{s)_id, random an account number, update account_holder table and accounts table
    # details in request body
    new_data = request.form
    customers = []
    for c in new_data['customer_id'].split(', '):
        customers.append(int(c))

    random_account_num = random.randint(10000, 99999)
    str_list = []
    values_list = []
    for field in new_data:
        if field == 'account_num':
            str_list.append(field)
            values_list.append(random_account_num)
        if field != 'customer_id' and field != 'account_num':
            str_list.append(field)
            values_list.append(new_data[field])

    sql = f"insert into accounts ({','.join(str_list)}) values(%s, %s, %s);"
    a_id_sql = f"select id from accounts where account_num = %s"
    ah_sql = f"insert into account_holder (account_id, customer_id) values (%s, %s)"

    with conn:
        with conn.cursor() as cur:
            cur.execute(sql, (tuple(values_list)))
            print(f"created account number {random_account_num}")
            if cur.rowcount == 1:
                updated_obj = "select * from accounts"
                cur.execute(updated_obj, )
                results = cur.fetchall()
                print(f"accounts: {results}")
            # get new_account_id:
            cur.execute(a_id_sql, (str(random_account_num), ))
            account_id = cur.fetchone()
            # add to account_holder table:
            counter = 0
            for customer in customers:
                cur.execute(ah_sql, (account_id, customer))
                if cur.rowcount == 1:
                    counter += 1
            if counter == len(customers):
                updated_ah = "select * from account_holder"
                cur.execute(updated_ah, )
                ah_results = cur.fetchall()
                print(f"account_holder: {ah_results}")
                return app.response_class(status=200)
    return app.response_class(status=500)


@app.route("/api/v1/accounts/<int:account_id>/deposit", methods=['POST'])
def deposit(account_id):
    # Body: amount
    new_data = request.form
    # request.form is kind of dictionary
    sql = f"update accounts set balance = balance + %s where id=%s"
    with conn:
        with conn.cursor() as cur:
            cur.execute(sql, tuple(new_data.values()) + tuple([account_id]))
            print(f"update account no. {account_id}")
            if cur.rowcount == 1:
                updated_obj = "select * from accounts"
                cur.execute(updated_obj, )
                results = cur.fetchall()
                print(results)
                return app.response_class(status=200)
    return app.response_class(status=500)


@app.route("/api/v1/accounts/<int:account_id>/withdraw", methods=['POST'])
def withdraw(account_id):
    # Body: amount. check there's enough money in account depends on limit
    # if OK - update account balance, update in transactions table
    new_data = request.form

    with conn:
        with conn.cursor() as cur:
            sql_money_in_account = f"select balance from accounts a where id = %s;"
            cur.execute(sql_money_in_account, (account_id,))
            balance = cur.fetchone()
            if balance:
                sql_limit = f"select max_limit from accounts a where id = %s;"
                cur.execute(sql_limit, (account_id,))
                limit = cur.fetchone()
                if limit:
                    if balance[0] - float(new_data['amount']) < limit[0]:
                        raise Exception("The withdrawal exceeds the limit")
                    sql = f"update accounts set balance = balance - %s where id=%s"
                    cur.execute(sql, tuple(new_data.values()) + tuple([account_id]))
                    print(f"withdraw from account no. {account_id}")
                    if cur.rowcount == 1:
                        updated_obj = "select * from accounts"
                        cur.execute(updated_obj, )
                        results = cur.fetchall()
                        print(f"accounts: {results}")
                        # update transactions table:
                        transaction_time = datetime.datetime.now()
                        update_transactions = f"insert into transactions " \
                                              f"(transaction_type, transaction_time, amount, " \
                                              f"performer_customer_id, performer_account_id) " \
                                              f"values('withdraw', '{transaction_time}', %s, " \
                                              f"(select ah.customer_id from account_holder ah " \
                                              f"where ah.account_id = {account_id})," \
                                              f"{account_id})"
                        cur.execute(update_transactions, tuple(new_data.values()))
                        if cur.rowcount == 1:
                            updated = "select * from transactions"
                            cur.execute(updated, )
                            transactions = cur.fetchall()
                            print(f"transactions: {transactions}")
                        return app.response_class(status=200)
                # no data for this customer_id
                return app.response_class(status=404)
    return app.response_class(status=500)
# check customer id is correct


@app.route("/api/v1/accounts/<int:account_id>/transfer", methods=['POST'])
def transfer(account_id):
    """
       The function gets query param (amount, receiving account and customer id -> The one who performs the transfer)
       and update the account balance if it possible
       """
    new_data = request.form
    print(new_data)
    transaction_time = datetime.datetime.now()

    sql_money_and_limit = f"select balance, max_limit from accounts a where id = %s;"
    sql_update_balance = f"update accounts set balance = %s where id=%s"
    sql_transactions = f"insert into transactions (transaction_type, transaction_time, " \
                       f"amount, performer_customer_id, performer_account_id, " \
                       f"receiver_account_id) values('transfer', %s, %s, %s, %s, %s)"
    with conn:
        with conn.cursor() as cur:
            cur.execute(sql_money_and_limit, (account_id,))
            sender_balance_and_limit = cur.fetchone()
            sender_balance = sender_balance_and_limit[0]
            sender_limit = sender_balance_and_limit[1]
            cur.execute(sql_money_and_limit, (new_data['receiver_account_id'], ))
            receiver_balance = cur.fetchone()
            receiver_balance = receiver_balance[0]

            if receiver_balance is None or sender_balance is None or \
                    sender_balance - float(new_data['amount']) < sender_limit:
                return app.response_class(status=404)

            # update sender account, receiver account:
            cur.execute(sql_update_balance, (sender_balance - float(new_data['amount']), account_id))
            cur.execute(sql_update_balance, (receiver_balance + float(new_data['amount']), new_data['receiver_account_id']))
            # update transactions table:
            cur.execute(sql_transactions, (f"{transaction_time}", new_data['amount'],
                                           new_data['costumer_id'], account_id, new_data['receiver_account_id']))
            if cur.rowcount == 1:
                updated = "select * from transactions"
                cur.execute(updated, )
                transactions = cur.fetchall()
                print(f"transactions: {transactions}")
                return app.response_class(status=200)
            return app.response_class(status=500)

@app.route("/api/v1/accounts/<int:account_id>", methods=['DELETE'])
def delete_account(account_id):
    print(f'called delete customer id no. {account_id}')
    with conn:
        with conn.cursor() as cur:
            sql = f"delete from accounts where id=%s"
            cur.execute(sql, (account_id,))
            if cur.rowcount == 1:
                updated_obj = "select * from accounts"
                cur.execute(updated_obj, )
                results = cur.fetchall()
                print(results)
                return app.response_class(status=200)
    return app.response_class(status=500)



if __name__ == '__main__':
    app.run(host="0.0.0.0", debug=True)
