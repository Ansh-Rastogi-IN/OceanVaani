# test_backend.py

from backend_chatbot import SimpleArgoChatbot

if __name__ == "__main__":
    bot = SimpleArgoChatbot()

    # 🔹 Example queries you can change freely
    queries = [
        "Show me temperature near Mumbai in 2018",
        "Get salinity near Chennai in 2015",
        "What is the pressure near Kolkata in 2020",
        "Give dissolved_oxygen data near Kochi in 2012",
    ]

    for q in queries:
        print("\n[QUERY]", q)
        df, sql = bot.run_query(q)
        print("[SQL]", sql)
        print(df.head())
