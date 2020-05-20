import sys
import pandas as pd


def main(excel_file,sheet_name,csv_file):
    df=pd.read_excel(excel_file,sheet_name, index_col=None,converters={'prov_plan_cd':str,'prov_npi':str,'bcbs_provi_id':str,'bdc_pgm':str,'bdc_sub_pgm':str,'eff_dt':str,'end_dt':str})
    #df.prov_npi=df.prov_npi.astype(int)
    df['bdc_pgm']=df['bdc_pgm'].apply('{:0>4}'.format)
    df['bdc_sub_pgm']=df['bdc_sub_pgm'].apply('{:0>4}'.format)
    df.to_csv('your_csv.csv', encoding='utf-8',index=False)

if __name__ == '__main__':
    excel_file=sys.argv[1]
    sheet_name=sys.argv[2]
    csv_file=sys.argv[3]
    main(excel_file,sheet_name,csv_file)
    
    

