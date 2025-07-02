import re

EXCLUDED_LABELS = [
    "Sony", "Universal", "Warner", "UMG", "T-Series", "Virgin", "Som Livre", "WM Brazil",
    "SM", "RCA", "BIGHIT", "Republic", "Epic", "Interscope", "under exclusive license",
    "Taylor Swift", "Netflix", "Coldplay", "Think its a game", "broke", "Copar", 
    "Double P Records", "Jonzing World", "CDLand", "Geffen records", "Big hit", "BMG", 
    "Spinnin", "Reprise", "Believe", "Because", "Empire", "Family Tree", "Native", 
    "Hot Girl Productions", "tommy boy", "LaFace", "Capitol", "Zee music", 
    "super cassettes", "Manorama", "Five Star Audio", "Plasma records", "muzik 247", 
    "times music", "bayshore", "think music", "sun pictures", "all ways dance", 
    "artiste first", "mom+pop", "zomba", "saregama", "polydor", "domino recordings", 
    "def jam", "island", "3qtr", "aditya music", "tips industries", "concord", 
    "10k projects", "Mass Appeal", "black 17 media", "oto8", "Atlantic", "WM", 
    "Robots & Humans", "TDE", "Columbia", "The System", "300 entertainment", "Cash Money"
]

def is_signed_label(label: str) -> bool:
    if not isinstance(label, str):
        return False
    label = label.lower()
    for name in EXCLUDED_LABELS:
        if name.lower() in label:
            return True
    return False

def filter_unsigned_tracks(df, label_column="Label"):
    return df[~df[label_column].apply(is_signed_label)]
