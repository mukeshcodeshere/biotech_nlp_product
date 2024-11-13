import os
import yfinance as yf
from flask import Flask, render_template, request, jsonify
import matplotlib.pyplot as plt
from io import BytesIO
import base64
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

app = Flask(__name__)

# Fetch the API_KEY securely from the environment
API_KEY = os.getenv('API_KEY')


def get_stock_data(tickers):
    stock_data = {}
    for ticker in tickers:
        stock = yf.Ticker(ticker)
        hist = stock.history(period="1mo")  # Get last month of data
        if not hist.empty:
            stock_data[ticker] = hist['Close']  # Store only the closing prices
        else:
            stock_data[ticker] = None
    return stock_data


def generate_chart(stock_data):
    fig, ax = plt.subplots(figsize=(10, 6))
    for ticker, data in stock_data.items():
        if data is not None:
            ax.plot(data.index, data.values, label=ticker)
    ax.set_xlabel('Date')
    ax.set_ylabel('Price (USD)')
    ax.set_title('Stock Price Data')
    ax.legend()
    fig.autofmt_xdate(rotation=45)

    # Save the plot to a BytesIO object and encode it to base64
    img = BytesIO()
    plt.tight_layout()
    plt.savefig(img, format='png')
    img.seek(0)
    img_base64 = base64.b64encode(img.getvalue()).decode('utf-8')
    plt.close(fig)
    return img_base64


@app.route('/')
def home():
    return render_template('index.html')


@app.route('/get_chart', methods=['POST'])
def get_chart():
    tickers = request.form['tickers'].split(',')
    stock_data = get_stock_data(tickers)
    chart = generate_chart(stock_data)
    return jsonify({'chart': chart, 'api_key': API_KEY})


if __name__ == '__main__':
    app.run(debug=True)
