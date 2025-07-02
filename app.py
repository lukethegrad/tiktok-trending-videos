from flask import Flask, request, jsonify
from spotify_scraper import get_spotify_label

app = Flask(__name__)

@app.route("/spotify_label", methods=["GET"])
def get_spotify_label_route():
    song = request.args.get("song")
    artist = request.args.get("artist")

    if not song or not artist:
        return jsonify({"error": "Missing 'song' or 'artist' query parameter"}), 400

    try:
        result = get_spotify_label(song, artist)

        # Catch unexpected None or bad return
        if not isinstance(result, dict):
            raise ValueError("get_spotify_label did not return a dictionary")

        return jsonify(result)

    except Exception as e:
        print("Error during label scrape:", e)
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
