import os
import pdfquery
import sys

def extract(data_dir):
    _, _, file_names = os.walk(data_dir).next()
    file_paths = [os.path.join(data_dir, filename) for filename in file_names if filename.endswith('.pdf')]
    print "Loading pdfquery for each pdf file."
    pdfs = [pdfquery.PDFQuery(pdf_path) for pdf_path in file_paths]
    for (pdf, path) in zip(pdfs, file_paths):
        print "Loading XML Tree for pdf %s" % path
        pdf.load()
        xml_path = path + ".xml"
        print "Writing tree for %s to %s" % (path, xml_path)
        pdf.tree.write(xml_path, pretty_print=True, encoding="utf-8")
    print "Finished writing PDF XML trees."

if __name__ == '__main__':
    extract(sys.argv[1])
