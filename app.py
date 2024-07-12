from flask import Flask, jsonify
import json

app = Flask(__name__)
FILE_NAME = 'kafka_data.json'

@app.route('/data', methods=['GET'])
def get_data():
    with open(FILE_NAME, 'r') as file:
        data = [json.loads(line) for line in file]
    return jsonify(data)

if __name__ == '__main__':
    app.run(debug=True)
