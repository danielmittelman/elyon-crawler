import sqlite3

class ElyonDatabase:
    conn = None
    full_text = False

    def __init__(self, filename, full_text):
        self.conn = sqlite3.connect(filename)
        self.full_text = full_text
        self.setup_database()

    def __del__(self):
        self.conn.close()

    # Verify that the database file exists and the requires tables exist. If not, create them
    def setup_database(self):
        cursor = self.conn.cursor()

        # Create the tables
        cursor.execute("CREATE TABLE IF NOT EXISTS verdicts ("
                       "row_id INTEGER PRIMARY KEY, "
                       "date TEXT, "
                       "case_id TEXT, "
                       "court TEXT, "
                       "subject TEXT, "
                       "status TEXT, "
                       "is_unanimous INTEGER, "
                       "court_fees_mandated INTEGER, "
                       "is_technical INTEGER, "
                       "verdict_type TEXT, "
                       "verdict_url TEXT)")
        cursor.execute("CREATE TABLE IF NOT EXISTS verdict_judges ("
                       "row_id INTEGER, "
                       "judge_name TEXT)")
        cursor.execute("CREATE TABLE IF NOT EXISTS verdict_parties ("
                       "row_id INTEGER, "
                       "side TEXT, "
                       "party_name TEXT, "
                       "legal_representation TEXT)")

        # If full text extraction is required, create a different table to store it
        if self.full_text:
            cursor.execute("CREATE TABLE IF NOT EXISTS verdict_fulltext ("
                           "row_id INTEGER, "
                           "text TEXT)")

        cursor.close()

    # Insert a CourtCase object into the database
    def insert_case(self, case):
        if self.conn is not None:
            cursor = self.conn.cursor()

            # Insert the basic metadata and retain the unique row_id
            cursor.execute("INSERT INTO verdicts "
                           "(date, court, subject, case_id, verdict_type, verdict_url, status,"
                           "is_technical, is_unanimous, court_fees_mandated) "
                           "VALUES(?,?,?,?,?,?,?,?,?,?)",
                           (case.date, case.group, case.subject, case.case_id,
                            case.doc_type, case.doc_url, case.status, case.technical,
                            case.is_unanimous, case.fees_mandated))
            row_id = str(cursor.lastrowid)

            # Insert the list of judges in the case to the appropriate table
            judges = [(j, ) for j in case.judges]
            cursor.executemany("INSERT INTO verdict_judges VALUES(" + row_id + ",?)", judges)

            # Insert the lists of present sides in court
            cursor.executemany("INSERT INTO verdict_parties VALUES(" + row_id + ",?,?,?)", case.sides)

            # If full text extraction is required, save the text as well (removing tabs)
            if self.full_text:
                cursor.execute("INSERT INTO verdict_fulltext VALUES(" + row_id + ",?)",
                               (case.doc_text.replace("\t", ""), ))

            # Commit changes and close the cursor
            self.conn.commit()
            cursor.close()