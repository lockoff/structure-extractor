import os
import pdfquery
import sys

def extract(data_dir):
    _, _, file_names = os.walk(data_dir).next()
    file_paths = [os.path.join(data_dir, filename) for filename in file_names if filename.endswith('.pdf')]
    print "Loading pdfquery for each pdf file."
    for path in file_paths:
        pdf = pdfquery.PDFQuery(path)
        print "Loading XML Tree for pdf %s" % path
        pdf.load()
        xml_path = path + ".xml"
        print "Writing tree for %s to %s" % (path, xml_path)
        pdf.tree.write(xml_path, pretty_print=True, encoding="utf-8")
        pdf.file.close()
    print "Finished writing PDF XML trees."

if __name__ == '__main__':
    extract(sys.argv[1])
