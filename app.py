from flask import Flask, jsonify
import pandas as pd
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

def calculate_co_occurrence(df):
    grouped = df.groupby('Question No')['Tags'].apply(lambda x: ','.join(x)).reset_index()
    python_questions = grouped[grouped['Tags'].str.contains('python', na=False)]
    co_occurrence_counts = {}
    for tags in python_questions['Tags']:
        tag_list = tags.split(',')
        for tag in tag_list:
            tag = tag.strip()
            if tag != 'python':
                co_occurrence_counts[tag] = co_occurrence_counts.get(tag, 0) + 1
    sorted_co_occurrences = sorted(co_occurrence_counts.items(), key=lambda x: x[1], reverse=True)
    top_10_tags = sorted_co_occurrences[:10]
    return top_10_tags

@app.route("/data")
def get_data():
    try:
        df = pd.read_csv("final_questions.csv")
        df.columns = df.columns.str.strip()
        df['Date'] = pd.to_datetime(df['Date'], dayfirst=True, errors='coerce')
        df.dropna(subset=['Date'], inplace=True)
        df['Month'] = df['Date'].dt.to_period('M').astype(str)
        df = df[df['Month'] >= '2021-10']
        monthly_data = df.groupby(['Month', 'Primary Tag']).size().unstack(fill_value=0)
        formatted_months = [pd.to_datetime(month).strftime('%b-%Y') for month in monthly_data.index]

        # Extract unique months directly from the original DataFrame
        unique_months = df['Month'].unique().tolist()
        formatted_unique_months = [pd.to_datetime(month).strftime('%b-%Y') for month in unique_months]

        python_tags = calculate_co_occurrence(df)
        count_data = {
            tag: {
                "months": formatted_months,
                "values": [int(val) for val in monthly_data[tag].tolist()]
            } for tag in monthly_data.columns
        }
        monthly_totals = monthly_data.sum(axis=1)
        percentage_data = {
            tag: {
                "months": formatted_months,
                "values": [
                    round((val / total) * 100, 2) if total != 0 else 0
                    for val, total in zip(monthly_data[tag], monthly_totals)
                ]
            } for tag in monthly_data.columns
        }
        volatile_data = {
            tag: {
                "months": formatted_months,
                "values": [int(val) for val in monthly_data[tag].diff().fillna(0).abs().tolist()]
            } for tag in monthly_data.columns
        }
        growth_data = {
            tag: {
                "months": formatted_months[1:],
                "values": [
                    round(((curr / prev) - 1) * 100, 2) if prev != 0 else 0
                    for prev, curr in zip(monthly_data[tag][:-1], monthly_data[tag][1:])
                ]
            } for tag in monthly_data.columns
        }
        latest_month = formatted_months[-1]
        latest_data = {
            tag: int(monthly_data[tag][-1]) for tag in monthly_data.columns
        }
        top_tags = sorted(latest_data.items(), key=lambda x: x[1], reverse=True)[:15]
        pie_data = {
            "labels": [tag for tag, _ in top_tags],
            "values": [value for _, value in top_tags]
        }

        return jsonify({
            "count": count_data,
            "percentage": percentage_data,
            "volatile": volatile_data,
            "growth": growth_data,
            "pie": pie_data,
            "python_tags": python_tags,
            "months": formatted_unique_months  # Send unique months to frontend
        })
    except Exception as e:
        return jsonify({"error": str(e)})

if __name__ == "__main__":
    app.run(debug=True)
