from flask import Flask, render_template, request, jsonify
import pandas as pd
from datetime import datetime
import os
from groq import Groq

app = Flask(__name__)

# =========================
# Load FAQ CSV
# =========================
FAQ_DF = pd.read_csv("portal_faq.csv")

# =========================
# LLM Setup
# =========================
client = Groq(api_key=os.getenv("GROQ_API_KEY"))

def llm_fallback(question):
    completion = client.chat.completions.create(
        model="llama3-70b-8192",
        messages=[
            {"role": "system", "content":
             "You are Genesis Portal Assistant. Answer only about portal usage, features and reports."},
            {"role": "user", "content": question}
        ],
        temperature=0.2
    )
    return completion.choices[0].message.content

# =========================
# FAQ Helpers
# =========================
def get_categories():
    return FAQ_DF["category"].unique().tolist()

def get_questions_by_category(category):
    return FAQ_DF[FAQ_DF["category"] == category][["question"]].to_dict("records")

def get_answer(question):
    row = FAQ_DF[FAQ_DF["question"].str.lower() == question.lower()]
    return None if row.empty else row.iloc[0]["answer"]

def auto_suggest(text):
    matches = FAQ_DF[FAQ_DF["question"].str.contains(text, case=False)]
    return matches["question"].head(5).tolist()

# =========================
# Logging
# =========================
def log_interaction(question, source, response):
    with open("chatbot_logs.csv", "a") as f:
        f.write(f"{datetime.now()},{question},{source},{response}\n")

# =========================
# Routes
# =========================
@app.route("/")
def home():
    return render_template("chatbot.html", categories=get_categories())

@app.route("/ask", methods=["POST"])
def ask():
    data = request.json
    question = data.get("question")

    # FAQ first
    answer = get_answer(question)
    if answer:
        log_interaction(question, "FAQ", answer)
        return jsonify({"reply": answer, "source": "faq"})

    # LLM fallback
    llm_reply = llm_fallback(question)
    log_interaction(question, "LLM", llm_reply)
    return jsonify({"reply": llm_reply, "source": "llm"})

@app.route("/questions", methods=["POST"])
def questions():
    category = request.json.get("category")
    return jsonify(get_questions_by_category(category))

@app.route("/suggest", methods=["POST"])
def suggest():
    text = request.json.get("text")
    return jsonify(auto_suggest(text))

if __name__ == "__main__":
    app.run(debug=True)

# from groq import Groq
# import os

# client = Groq(api_key=os.getenv("GROQ_API_KEY"))

# def robot_ai_response(user_message):
#     completion = client.chat.completions.create(
#         model="llama3-70b-8192",
#         messages=[
#             {"role": "system", "content": "You are a helpful data quality assistant."},
#             {"role": "user", "content": user_message}
#         ],
#         temperature=0.4
#     )

#     return completion.choices[0].message.content
