"""
Driver script to execute the Feature extraction pipeline to generate SCORE datasets

Pre-requisite if running on a Windows machine:
    1. Install the 64-bit version of XPDF from https://www.xpdfreader.com/download.html
    2. Specify the full path to the pdftotext binary using the PDFTOTEXT_PATH variable in app_config.yaml
    It should be something like this: <user_installation directory/xpdf-tools-win-4.02\bin64\pdftotext
"""

from Feature_Pipeline.utilities import read_darpa_tsv, csv_writer, csv_write_field_header, csv_write_record, select_keys, \
    tamu_select_features
from Feature_Pipeline.grobid_client.grobid_client import run_grobid
from Feature_Pipeline.extractor import TEIExtractor
from Feature_Pipeline.p_value import extract_p_values
# from Feature_Pipeline.p_value_scifact import extract_p_values_sc
from Feature_Pipeline.tamu_features.adapter import get_tamu_features
from Feature_Pipeline.theory import extract_theory
from Feature_Pipeline.negation import extract_neg
from Feature_Pipeline.cite_intent import extract_ci
from Feature_Pipeline.issn_sub_doi import getsub, extract_issn_sub, get_issn, get_sjr_ven, get_scr_sub, get_issn_doi
from Feature_Pipeline.tabfig import get_tabfig
from Feature_Pipeline.claimtabfig import get_claimtabfig

# from Feature_Pipeline.statcheckerr import extract_statck
from collections import namedtuple
from os import listdir, rename, system, name, path, getcwd
from Feature_Pipeline.databases import Database
import time
import argparse
import random
import traceback
import pandas as pd
# TODO: Ajay add these packages to requirements.txt if they're being used
import logcontrol
import timelogger
import yaml
import re
import os

# Note that the script assumes that the root directory is set to score_app
config_file = open(r'app_config.yaml')
config = yaml.safe_load(config_file)
windows_pdftotext_path = '/Users/rajratnpranesh/Score/xpdf-tools-linux-4.03/bin64/pdftotext'

if __name__ == "__main__":

    # The driver script for the feature pipeline is designed to be invoked as
    # a python shell command with the following configurable options
    parser = argparse.ArgumentParser(description="Pipeline - Process PDFS - Market Pre-processing")
    # Directory containing all the PDFs to be processed
    parser.add_argument("-in", "--pdf_input", help="parent folder that contains all pdfs")
    # Output directory for GROBID generated TEI XMLs
    parser.add_argument("-out", "--grobid_out",  help="grobid output path")
    # The pipeline supports several modes: process-pdfs, generate-train, extract-test, 2400set
    # TODO: Ajay to document the 2400set section. Recommend better generalization support to combine generate-train
    #  and 2400set
    parser.add_argument("-m", "--mode", default="extract-test", help="pipeline mode")
    # GROBID concurrency configuration
    # TODO: Generalize to process datapoints in parallel to improve throughput
    parser.add_argument("-n", default=1, help="concurrency for service usage")
    # Metadata file to generate ordered dataset - predominantly used for test, extended to use for train set as well
    parser.add_argument("-meta", "--meta", help="DARPA metadata tsv/csv for test")
    # Specifies the output path for the generated dataset saved as a CSV
    parser.add_argument("-csv", "--csv_out", default=getcwd(), help="CSV output path")
    # Specifies the label to be set for a training set data point. Built-in with the replication project/retraction
    # papers etc. in mind
    parser.add_argument("-l", "--label", help="Assign y value | label for training set")
    # Sets a label randomly chosen between an acceptable defined range
    parser.add_argument("-lr", "--label_range", help="Assign y value within range for training set | Ex: 0.7-1")

    # python process_docs.py -out ../../tei10 -in ../../pdf10 -m generate-train" -csv ../
    # Persists certain values in a SQLite database to avoid repeated computation if entry exists in db, think
    # memoization based on an identifier. Co-citation is an example of a feature stored in the DB.
    # TODO: Ajay please make the database an optional configuration parameter, also it would be helpful to list the
    #  features that are being stored in a README file
    database_path = path.expanduser('~/Score/data/database')
    database = Database(database_path)

    args = parser.parse_args()

    # Initialize loggers
    logcontrol.register_logger(timelogger.logger, "timelogger")
    logcontrol.set_level(logcontrol.DEBUG, group="timelogger")
    # logcontrol.log_to_console(group="timelogger")
    logcontrol.set_log_file(args.csv_out + '/main_log.txt')

    # Debug parameters config to test pipeline locally
    # args.mode = "extract-test"
    # args.grobid_out = r"C:\Users\arjun\dev\GROBID_processed\test"
    # args.pdf_input = r"C:\Users\arjun\dev\test\pdfs"
    args.data_file = r"/Users/rajratnpranesh/Score/darpa-market/score_app/data.csv"
    # args.csv_out = r"C:\Users\arjun\dev"

    # This mode processes PDFs through GROBID and pdftotext to generate the TEI XMLs and .txt files for papers
    if args.mode == "process-pdfs":
        # Change pdf names (Some PDFs have '-' instead of '_' in the names: Relevant to TA2 data/papers)
        for count, filename in enumerate(listdir(args.pdf_input)):
            print("Processing: ", filename, ", file number: ", count)
            new_name = filename.replace('-', '_')
            rename(args.pdf_input+'/'+filename, args.pdf_input+'/'+new_name)
            # Generate text files from PDFs. Note that txt files are generated in the same directory as the PDFs
            if name == 'nt':
                command = r"{0} {1}/".format(windows_pdftotext_path, args.pdf_input) + filename
            else:
                command = "pdftotext {0}/{1}".format(args.pdf_input, filename)
            # Execute system calls/shell type commands in an OS subshell environment
            system(command)
        # Generate TEI XMLS after processing through GROBID
        run_grobid(args.pdf_input, args.grobid_out, args.n)

    # Generate Training data
    elif args.mode == "generate-train":
        # Read features list and append label for CSV writer
        id_list = []
        feature_list = config['PIPELINE']['FEATURES']
        feature_list.append('y')
        fields = tuple(feature_list)
        # fields = tuple(feature_list.append('y'))

        record = namedtuple('record', fields)
        record.__new__.__defaults__ = (None,) * (len(feature_list) + 1)
        # CSV output file (Delete the file manually if you wish to generate fresh output, default appends
        if path.isfile(args.csv_out + "/train.csv"):
            write_head = False
        else:
            write_head = True
        writer = csv_writer(r"{0}/{1}".format(args.csv_out, "train.csv"), append=True)
        header = list(fields)
        if write_head:
            csv_write_field_header(writer, header)
        # Run pipeline
        xmls = listdir(args.grobid_out)
        # print('################################')
        # xmls.remove('.DS_Store')
        # print(xmls)
        # print('################################')
        for xml in xmls:
            try:
                print("Processing ", xml)
                pdic = {}
                extractor = TEIExtractor(args.grobid_out + '/' + xml, database)
                # print('################################')
                # print(args.grobid_out + '/' + xml)
                # print('################################')
                extraction_stage = extractor.extract_paper_info()
                issn = extraction_stage['ISSN']
                auth = extraction_stage['authors']
                citations = extraction_stage['citations']
                scr = extraction_stage['scr']
                doi = extraction_stage['doi']

                # print("######################scr")
                # print(type(scr))
                # print("######################scr")


                # body = extraction_stage['body']

                # print("######################scr")
                # print(scr)
                # print("######################scr")
                p_val_stage = extract_p_values(args.pdf_input + '/' + xml.replace('.tei.xml', '.txt'))
                tfile = xml.replace('.tei.xml', '.txt')
                s = re.split("_|\.",tfile)
                # issn = int(issn)
                # print("######################")
                # print(issn)
                # print("######################")
                if issn!= str(0):
                    # print("CALL 1")
                    # issn = str(issn)
                    if issn[-1] == 'X':
                        sub_dict = extract_issn_sub(issn)# call issn_sub
                        venue_feat = get_sjr_ven(issn)# call venue_sjr_get
                    else:
                        strr = re.sub("^0+(?!$)", "", issn)
                        issn = re.sub("[^a-zA-Z0-9 ]", "", strr)
                        sub_dict = extract_issn_sub(issn)# call issn_sub
                        venue_feat = get_sjr_ven(issn)# call venue_sjr_get
                # elif doi != None:
                #     issn_new = get_issn(doi)# call crossref and get issn, use regex to Cleanup
                #     issn_new = str(issn_new)
                #     if issn_new[-1] == 'X':
                #         sub_dict = extract_issn_sub(issn_new)# call issn_sub
                #         venue_feat = get_sjr_ven(issn_new)# call venue_sjr_get
                #     else:
                #         strr = re.sub("^0+(?!$)", "", issn_new)
                #         issn_new = re.sub("[^a-zA-Z0-9 ]", "", strr)
                #         sub_dict = extract_issn_sub(issn_new)# call issn_sub
                #         venue_feat = get_sjr_ven(issn_new)# call venue_sjr_get

                else:
                    doi = 'DOI'
                    # doi = get_issn_doi(s[3])# call crossref and get issn, use regex to Cleanup
                    #
                    # issn_new = get_issn(str(doi))
                    # issn_new = str(issn_new)
                    # print("print",doi,issn_new)
                    # if issn_new[-1] == 'X':
                    #     sub_dict = extract_issn_sub(issn_new)# call issn_sub
                    #     venue_feat = get_sjr_ven(issn_new)# call venue_sjr_get
                    # else:
                    #     strr = re.sub("^0+(?!$)", "", issn_new)
                    #     issn_new = re.sub("[^a-zA-Z0-9 ]", "", strr)
                    #     sub_dict = extract_issn_sub(issn_new)# call issn_sub
                    #     venue_feat = get_sjr_ven(issn_new)# call venue_sjr_get

                try:
                    scr_scr_sub = get_scr_sub(int(scr))
                except:
                    scr_scr_sub = sub_dict
                    print("No scr")



                del extraction_stage['ISSN']
                del extraction_stage['authors']
                del extraction_stage['citations']
                del extraction_stage['scr']
                # del extraction_stage['doi']
                # del extraction_stage['body']
                p_val_stage = extract_p_values(args.pdf_input + '/' + xml.replace('.tei.xml', '.txt'))
                tfile = xml.replace('.tei.xml', '.txt')
                s = re.split("_|\.",tfile)
                pdic['paper_id_out'] = s[3]
                theory_stage = extract_theory(s[3])
                neg_stage = extract_neg(s[3])
                claimft_dict = get_claimtabfig(s[3])
                # statcheckerr_pval = extract_statck_pval(s[3])
                # p_val_stage_sc = extract_p_values_sc(s[3])    ## Scifact p_value
                # statck_stage = extract_statck(s[3])
                ci_stage = extract_ci(s[3])
                # oads_stage = extract_oads(s[3])

                ctf_filepath = args.grobid_out + '_ctf/' + xml.replace('.tei.xml', '.xml')
                grobid_filepath = args.grobid_out + '/' + xml.replace('.tei.xml', '.tei.xml')

                if os.path.isfile(ctf_filepath):
                    file_type = "ctf"
                    ft_dict = get_tabfig(file_type,ctf_filepath)
                else:
                    file_type = "grobid"
                    ft_dict = get_tabfig(file_type,grobid_filepath)




                # print("#############################################")
                # print("#############################################")
                # print(ci_stage)
                # # print(body)
                # print("#############################################")
                # print("#############################################")
                # oo = xml.replace('.tei.xml', '.txt')
                # print(p_val_stage)
                # print(oo)
                # print("#############################################")
                # print("#############################################")
                # features = dict(**extraction_stage, **p_val_stage, **theory_stage, **neg_stage, **p_val_stage_sc, **ci_stage)
                features = dict(**extraction_stage, **p_val_stage, **theory_stage, **neg_stage, **ci_stage, **sub_dict, **venue_feat, **ft_dict)
                # features = dict(**extraction_stage, **theory_stage)
                # pdb.set_trace()
                # Get TAMU features
                paper_id = xml.split('_')[-1].replace('.tei.xml', '')
                id_list.append(paper_id)
                tamu_features = get_tamu_features(args.data_file, paper_id, issn, auth, citations,database)
                select_tamu_features = select_keys(tamu_features, tamu_select_features)
                features.update(select_tamu_features)
                print(features)
                if args.label_range:
                    label_range = args.label_range.split('-')
                    features['y'] = random.uniform(float(label_range[0]), float(label_range[1]))
                else:
                    features['y'] = float(args.label)
                # features['y'] = 1
                try:
                    csv_write_record(writer, features, header)
                except UnicodeDecodeError:
                    print("CSV WRITE ERROR", features["ta3_pid"])
                except UnicodeEncodeError:
                    print("CSV WRITE ERROR", features["ta3_pid"])
            except Exception as e:
                print(str(e))
                print(traceback.format_exc())

        # print(id_list)
        dflist = pd.DataFrame(id_list, columns=['id'])   ## get the id of paper in sequence
        dflist.to_csv('/Users/rajratnpranesh/Score/darpa-market/score_app/out_csv/id.csv')

    # Generate DARPA SCORE Test set
    elif args.mode == "extract-test":
        start = time.time()

        # Read features list and append label for CSV writer
        feature_list = config['PIPELINE']['FEATURES']
        fields = tuple(feature_list)

        record = namedtuple('record', fields)
        record.__new__.__defaults__ = (None,) * len(record._fields)
        # CSV output file
        writer = csv_writer(r"{0}/{1}".format(args.csv_out, "test.csv"))
        header = list(fields)
        csv_write_field_header(writer, header)
        args.data_file = args.file
        for document in read_darpa_tsv(args.file):
            try:
                print("Processing ", document['pdf_filename'])
                extractor = TEIExtractor(args.grobid_out + '/' + document['pdf_filename'] + '.tei.xml', document)
                extraction_stage = extractor.extract_paper_info()
                p_val_stage = extract_p_values(args.pdf_input + '/' + document['pdf_filename'] + '.txt',
                                               document['claim4'])
                features = dict(**extraction_stage, **p_val_stage)
                issn = extraction_stage['ISSN']
                auth = extraction_stage['authors']
                citations = extraction_stage['citations']
                del extraction_stage['ISSN']
                del extraction_stage['authors']
                del extraction_stage['citations']
                # Get TAMU features
                paper_id = document['pdf_filename'].split('_')[-1].replace('.pdf', '')
                # TAMU
                tamu_features = get_tamu_features(args.data_file, paper_id, issn, auth, citations, database)
                select_tamu_features = select_keys(tamu_features, tamu_select_features)
                # tamu_features, imputed_list = get_tamu_features(args.file, paper_id, extraction_stage[''])
                # select_tamu_features = select_keys(tamu_features, tamu_select_features)
                features.update(select_tamu_features)

                features['ta3_pid'] = document['ta3_pid']
                try:
                    csv_write_record(writer, features, header)
                except UnicodeDecodeError:
                    print("CSV WRITE ERROR", features["ta3_pid"])
                except UnicodeEncodeError:
                    print("CSV WRITE ERROR", features["ta3_pid"])
            except Exception as e:
                print(str(e))
                print(traceback.format_exc())
        end = time.time()
        print("Execution time: ", end-start)
    elif args.mode == "2400set":
        csv = pd.read_csv(args.file)
        want = ['kw_cs_m5', 'kw_cs_m3', 'kw_cs_m10', 'um_cs_m1', 'um_cs_m2', 'um_cs_m3', 'um_cs_m4', 'paper_id']
        # Read features list and append label for CSV writer
        feature_list = config['PIPELINE']['FEATURES']
        fields = tuple(feature_list)

        fields = fields + tuple(want)
        record = namedtuple('record', fields)
        record.__new__.__defaults__ = (None,) * len(record._fields)
        # CSV output file (Delete the file manually if you wish to generate fresh output, default appends
        if path.isfile(args.csv_out + "/2400train.csv"):
            write_head = False
        else:
            write_head = True
        writer = csv_writer(r"{0}/{1}".format(args.csv_out, "2400train.csv"), append=True)
        header = list(fields)
        if write_head:
            csv_write_field_header(writer, header)
        # Run pipeline
        xmls = listdir(args.grobid_out)
        for xml in xmls:
            try:
                timelogger.start("overall")
                print("Processing ", xml)
                # pdb.set_trace()
                timelogger.start("metadata_apis")
                extractor = TEIExtractor(args.grobid_out + '/' + xml, database)
                extraction_stage = extractor.extract_paper_info()
                issn = extraction_stage['ISSN']
                auth = extraction_stage['authors']
                citations = extraction_stage['citations']
                del extraction_stage['ISSN']
                del extraction_stage['authors']
                del extraction_stage['citations']
                timelogger.stop("metadata_apis")

                timelogger.start("p-value")
                p_val_stage = extract_p_values(args.pdf_input + '/' + xml.replace('.tei.xml', '.txt'))
                features = dict(**extraction_stage, **p_val_stage)
                timelogger.stop("p-value")

                # Get TAMU features
                timelogger.start("tamu")
                paper_id = xml.split('_')[-1].replace('.xml', '')
                tamu_features = get_tamu_features(args.data_file, paper_id, issn, auth, citations, database)
                select_tamu_features = select_keys(tamu_features, tamu_select_features)
                features.update(select_tamu_features)
                timelogger.stop("tamu")
                print(features)
                if args.label_range:
                    label_range = args.label_range.split('-')
                    features['y'] = random.uniform(float(label_range[0]), float(label_range[1]))
                else:
                    # pdb.set_trace()
                    for i in want:
                        features[i] = csv[csv['pdf_filename'] == xml.replace('.tei.xml', '').strip()][i].values[0]

                try:
                    csv_write_record(writer, features, header)
                except UnicodeDecodeError:
                    print("CSV WRITE ERROR", features["ta3_pid"])
                except UnicodeEncodeError:
                    print("CSV WRITE ERROR", features["ta3_pid"])
            except Exception as e:
                print(str(e))
                print(traceback.format_exc())
            finally:
                timelogger.stop("overall")
