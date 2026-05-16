from flask import Flask, request

app = Flask(__name__)

@app.route('/healthSync', methods=['POST'])
def health_sync():
    data = request.get_json(force=True, silent=True)
    if data is None:
        data = request.data.decode('utf-8')
    print('Received data:', data)
    return {'status': 'received'}, 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5050, debug=True)
