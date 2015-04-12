import urllib.request
import re

class CourtCase:
    date = ""
    group = ""
    subject = ""
    case_id = ""
    doc_type = ""
    judges = []
    sides = []
    status = ""
    doc_url = ""
    technical = False
    doc_text = None  # Plain text
    is_unanimous = 1
    fees_mandated = 0

    def __init__(self, xml=None, extended_info=None, text=None):
        if xml is not None and extended_info is not None:
            self.date = xml.find("OWSVERDICTDATE").text[:10]
            self.group = xml.find("OWSCASENUMBER").text.split(" ")[0]
            self.case_id = xml.find("OWSCASENUMBER").text.split(" ")[1]
            self.subject = xml.find("OWSCASENAME").text
            self.doc_type = xml.find("OWSVERDICTTYPE").text
            self.doc_url = xml.find("OWSDOCSFOLDERPATH").text + "/" + xml.find("OWSVERDICTFILENAME").text
            self.status = extended_info[0]
            self.sides = extended_info[1]
            self.judges = extended_info[2]
            self.technical = (xml.find("OWSISTECHNICAL").text == "True")
            self.doc_text = text

            # Also calculate the following fields
            self.is_unanimous = CourtCase.get_decision_rendered_unanimously(text)
            self.fees_mandated = CourtCase.get_court_fees_mandated(text)

    @staticmethod
    def extract_filename(xml):
        return xml.find("OWSDOCSFOLDERPATH").text + "/" + xml.find("OWSVERDICTFILENAME").text

    @staticmethod
    def extract_case_id(xml):
        return xml.find("OWSCASENUMBER").text.split(" ")[1]

    @staticmethod
    def get_document_text(doc_url, timeout):
        try:
            res = urllib.request.urlopen(doc_url, timeout=timeout)
            if not(res.code == 200):
                return ""
        except:
            return ""

        return res.read().decode("cp1255")

    # Returns whether the court ordered one of the parties to pay the court fees
    @staticmethod
    def get_court_fees_mandated(text):
        # If no text was passed, return None so the field in the database would remain empty
        if text is None or len(text) == 0:
            return -1

        # Compile the regular expressions representing positive and negative court fees decisions
        negative_regex = re.compile(r'(אין|לא|אינ).{1,25}צו ל*הוצאות', re.M | re.U)
        positive_regex = re.compile(r'(ישלם|ישלמ|תשלמ|ישא|תישא|תשא|יפצ|תפצ).{1,40}(הוצאות|שכ\"ט|שכר).+(ב?סך|ב?סה\"כ|ב?סכום)',
                                    re.M | re.U)

        # Search the text for all occurrences of the expressions above
        neg = re.findall(negative_regex, text)
        pos = re.findall(positive_regex, text)

        # Count the number of occurrences for each expression
        neg_count = 0 if neg is None else len(neg)
        pos_count = 0 if pos is None else len(pos)

        if neg_count > 0 and pos_count == 0:  # If only negative expressions were found, return false
            return 0
        elif neg_count == 0 and pos_count > 0:  # If only positive expressions were found, return true
            return 1
        elif neg_count == 0 and pos_count == 0:  # If no expressions were found, return false
            return 0
        else:
            # If both positive expressions and negative expressions were found, refocus the search
            # on the last 40% of the text. Try the same calculation again
            refocus_length = int((len(text) * 0.4) * (-1))
            neg = re.findall(negative_regex, text[:refocus_length])
            pos = re.findall(positive_regex, text[:refocus_length])
            neg_count = 0 if neg is None else len(neg)
            pos_count = 0 if pos is None else len(pos)

            if neg_count > 0 and pos_count == 0:
                return 0
            elif neg_count == 0 and pos_count > 0:
                return 1
            elif neg_count == 0 and pos_count == 0:
                return 0
            else:
                return -1  # If unable to determine, set as undetermined

    # Returns 1 if the decision was rendered unanimously, or 0 if the decision
    # was rendered on a majority basis.
    @staticmethod
    def get_decision_rendered_unanimously(text):
        if text is None or len(text) == 0:
            return -1

        majority_regex = re.compile(r'הוחלט.{1,16}ב?(רוב).{1,16}דעות', re.M | re.U)
        majority_regex2 = re.compile(r'הוחלט.{1,16}ב?(דעת).{1,16}(רוב)', re.M | re.U)
        majority_regex3 = re.compile(r'הוחלט.{1,50}נגד.?(חוות)?.?דעתו.{1,16}חולק', re.M | re.U)

        if re.search(majority_regex, text) is not None or \
                re.search(majority_regex2, text) is not None or \
                re.search(majority_regex3, text) is not None:
            return 0
        else:
            return 1