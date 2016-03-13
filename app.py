import csv
import psycopg2
from io import StringIO
from psycopg2.extras import RealDictConnection
from functools import wraps
from flask import Flask, render_template, redirect, url_for, request, session, \
    flash, make_response

from config_data.db_conn_parameters import local

app = Flask(__name__)
app.config['DATABASE_URI'] = local


def login_required(f):
    @wraps(f)
    def wrap(*args, **kwargs):
        if 'logged_in' in session:
            return f(*args, **kwargs)
        else:
            flash('You need to login first!')
            return redirect(url_for('login'))
    return wrap


@app.route('/')
def home():
    header = ['Departament', 'Title', 'Description', 'Edit entry', 'Run query']
    stmt = "SELECT q.id, d.name, q.name, q.description " \
           "FROM query q JOIN departments d ON q.department_id=d.id " \
           "ORDER BY 2,3;"
    with psycopg2.connect(dsn=local) as conn:
        with conn.cursor() as cursor:
            cursor.execute(stmt)
            result = cursor.fetchall()
    return render_template('index.html', header=header, rows=result)


@app.route('/welcome')
def welcome():
    return render_template('welcome.html')


@app.route('/new', methods=['GET'])
def new():
    stmt = "WITH a AS (INSERT INTO query (name, department, description, query)" \
           "VALUES (%s,%s,%s,%s) RETURNING id) SELECT id FROM a;"

    if request.args.get('save', '').strip():

        name = request.args.get('name', '').strip()
        department = request.args.get('department', '').strip()
        description = request.args.get('description', '').strip()
        query = request.args.get('query', '').strip()

        with psycopg2.connect(dsn=local) as conn:
            with conn.cursor() as cursor:
                cursor.execute(stmt, (name, department, description, query))
                id = cursor.fetchone()[0]
                conn.commit()
        return redirect(url_for('results', id=id))
    else:
        return render_template('new.html')


@app.route('/edit/<int:id>', methods=['GET'])
def edit(id):
    select_stmt = "SELECT q.name, d.name as department, q.query, q.description " \
                  "FROM query q JOIN departments d ON q.department_id = d.id WHERE q.id=%s;"
    update_stmt = "UPDATE query SET description=%s, query=%s WHERE id=%s;"
    delete_stmt = "DELETE FROM query WHERE id=%s;"

    if request.args.get('save', '').strip():
        description = request.args.get('description', '').strip()
        query = request.args.get('query', '').strip()

        with psycopg2.connect(dsn=local) as conn:
            with conn.cursor() as cursor:
                cursor.execute(update_stmt, (description, query, str(id)))
                conn.commit()
        return redirect(url_for('home'))

    elif request.args.get('delete', '').strip():
        with psycopg2.connect(dsn=local) as conn:
            with conn.cursor() as cursor:
                cursor.execute(delete_stmt, (str(id), ))
                conn.commit()
        return redirect(url_for('home'))

    else:
        with RealDictConnection(dsn=local) as conn:
            with conn.cursor() as cursor:
                cursor.execute(select_stmt, (str(id), ))
                res = cursor.fetchone()
        return render_template('edit.html', details=res)


@app.route('/query/<int:id>')
def results(id):
    select_stmt = "SELECT q.name, d.name as department, q.description, q.query " \
                  "FROM query q JOIN departments d ON q.department_id = d.id " \
                  "WHERE q.id=%s;"
    with RealDictConnection(dsn=local) as conn:
        with conn.cursor() as cursor:
            cursor.execute(select_stmt, (str(id), ))
            res = cursor.fetchone()
    if res:
        with RealDictConnection(dsn=local) as conn:
            with conn.cursor() as cursor:
                cursor.execute(res['query'])
                result = cursor.fetchall()
                header = result[0].keys()
        if request.args.get('download', '').strip():
            si = StringIO()
            f = csv.writer(si)
            f.writerow(header)
            f.writerows([row.values() for row in result])
            output = make_response(si.getvalue())
            output.headers["Content-Disposition"] = "attachment; filename=%s.csv" \
                                                    % str(res['name'])
            output.headers["Content-type"] = "text/csv"
            return output
        else:
            return render_template('results.html',
                                   details=res, rows=result[0:5], id=id,
                                   header=header)
    else:
        return 'Query with id %s does not exist!' % str(id)


if __name__ == '__main__':
    app.run(debug=True)
