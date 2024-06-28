from flask import Flask, request, render_template, url_for, jsonify
from base_class.base_func import batch_delete_data_func

app = Flask(__name__, static_folder="static", static_url_path="/static")
app.config['JSON_AS_ASCII'] = False


## 前端用户界面，通过前端SDK操作
@app.route('/', methods=['GET'])
def index():
  return render_template('index.html')


## 自动化流程接口，通过后端SDK操作
@app.route('/batch_delete_data', methods=['POST'])
def batch_delete_data():

  data = request.get_data().decode()
  # print(data)

  try:
    result = batch_delete_data_func(data)
    code = 200

  except Exception as e:
    # result = '数据删除失败，请检查后重试'
    result = str(e)
    code = -1

  return {"code": code, "msg": result}


app.run(host='0.0.0.0', port=3300, debug=True, use_reloader=True)
