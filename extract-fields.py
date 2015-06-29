import os
import sys
import pdfquery
import json

def extract(data_dir):
    _, _, file_names = os.walk(data_dir).next()
    file_paths = [os.path.join(data_dir, filename) for filename in file_names if filename.endswith('.pdf')]
    print "Loading pdfquery for each pdf file."
    pdfs = [pdfquery.PDFQuery(pdf_path) for pdf_path in file_paths]
    selector = 'LTTextBoxHorizontal:contains("%s") LTTextLineHorizontal:last-child'
    for (pdf, path) in zip(pdfs, file_paths):
        print "Loading XML Tree for pdf %s" % path
        pdf.load()
        json_path = path + '.json'
        print "Extracting fields for pdf %s" % path
        fields = pdf.extract([
            ('with_formatter', 'text'),
            ('property_type', selector % "Property Type"),
            ('building_type', selector % "Building Type"),
            ('storeys', selector % "Storeys"),
            ('community_name', selector % "Community Name"),
            ('title', selector % "Title"),
            ('land_size', selector % "Land Size"),
            ('parking_type', selector % "Parking Type"),
            ('amenities_nearby', selector % "Amenities Nearby"),
            ('features', selector % "Features"),
            ('parking_type', selector % "Parking Type"),
            ('total_parking_spaces', selector % "Total Parking Spaces"),
            ('basement_features', selector % "Basement Features"),
            ('basement_type', selector % "Basement Type"),
            ('bathrooms_total', selector % "Bathrooms (Total)"),
            ('bathrooms_partial', selector % "Bathrooms (Partial)"),
            ('bedrooms_above_grade', selector % "Bedrooms - Above Grade"),
            ('bedrooms_below_grade', selector % "Bedrooms - Below Grade"),
            ('cooling', selector % "Cooling"),
            ('exterior_finish', selector % "Exterior Finish"),
            ('heating_fuel', selector % "Heating Fuel"),
            ('heating_type', selector % "Heating Type"),
            ('style', selector % "Style")
        ])
        print "Writing fields for pdf %s to json file %s" % (path, json_path)
        with open(json_path, 'w') as fp:
            json.dump(fields, fp, indent=4)

if __name__ == '__main__':
    extract(sys.argv[1])

