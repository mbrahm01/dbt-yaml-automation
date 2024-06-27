import yaml
import os 
import pandas as pd
import numpy as np
import re
#import xlrd
#path_1='C://Users//mbrahm01//Documents'
#path='C://Users//mbrahm01//Documents//RAW_VAULT_CLINICAL_CAREGAPS_STTM_OCMTM.xlsx'
pd.options.mode.chained_assignment=None
def main(path_1):
    sheets_main=['STAGE_RAWVAULT','LOAD_TO_STAGE','RV_TO_IM','LOAD_TO_RV','LOAD_TO_RAWVAULT - FLAG EV.','LOAD_TO_RAWVAULT','RAWVAULT_BV']
    for files in os.listdir(path_1):
        if files.endswith('.xlsx'):
            path=f'{path_1}//{files}'
            excel=pd.ExcelFile(path)
            sheet_excel=excel.sheet_names
            sheets=list(set(sheets_main)&set(sheet_excel))
            for sheet in sheets:
                try:
                    #print(sheet)
                    df_info=pd.read_excel(path,sheet_name=sheet)
                    df_info['field_name']=df_info['field_name'].str.strip()
                    df_info['datatype']=df_info['datatype'].replace(',',' ',regex=True)
                    dirt=df_info.groupby('Target: Table/API')['field_name'].agg(list).to_dict()
                    ddl=df_info.groupby('Target: Table/API').agg({'field_name':list,'datatype':list,'Not_null/Null':list}).to_dict()
                    #ddl
                    ddl_field_name=ddl['field_name']
                    ddl_type=ddl['datatype']
                    #print(df_info['datatype'])
                    ddl_not_null=ddl['Not_null/Null']
                    key=['PK','pk','FK','fk','PK, FK','pk, fk','Y','BK']
                    df_dup=df_info[df_info['Target: PK/FK'].isin(key)]
                    df_dup_main=df_dup.groupby('Target: Table/API')['field_name'].agg(list).to_dict()
                    #merged={k:ddl_field_name.get(k,[])+ ddl_type.get(k,[]) for k in set(ddl_field_name)|set(ddl_type)}
                    zepped={k:list(zip(ddl_field_name[k],ddl_type[k])) for k in set(ddl_field_name)|set(ddl_type)}
                    #print(ddl)
                    default=df_info[df_info['Default_Values'].notna()]
                    default_main=default.groupby('Target: Table/API').agg({'field_name':list,'Default_Values':list}).to_dict()
                    default_field=default_main['field_name']
                    default_to=default_main['Default_Values']
                    default_fin={k:list(zip(default_field[k],default_to[k])) for k in set(default_field)|set(default_to)}
                    #print(default_fin)
                    
                    #data
                    data_val=df_info.groupby('Target: Table/API').agg({'field_name':list,'SourceTable':list,'Source Field Name':list}).to_dict()
                    #print(data_val)
                    
                    data_val_name=data_val['field_name']
                    data_val_src_table=data_val['SourceTable']
                    data_val_source_column=data_val['Source Field Name']
                    zipped_data={k:list(zip(data_val_name[k],data_val_src_table[k],data_val_source_column[k])) for k in set(data_val_name)|set(data_val_src_table)|set(data_val_source_column)}
                    #print(zipped_data)
                    df_tgt_db=df_info[df_info['Target Data Base'].notnull() &(df_info['Target Data Base']!='')]['Target Data Base'].dropna()
                    df_tgt_db=df_tgt_db.reset_index(drop=True)
                    #print(df_tgt_db[0])
                    df_tgt_db_first=df_tgt_db[0]
                    df_tgt_db_main=df_tgt_db_first.split('.')
                    #print(df_src_schema)
                    df_tgt_db_main=df_tgt_db_main[0].replace('PRD1','QA1')
                    
                    scema='RAW_VAULT'
                    if sheet=='LOAD_TO_STAGE':
                        scema='STAGE'
                    elif sheet=='RV_TO_IM':
                        scema='TARGET_DAL'
                    elif sheet=='RAWVAULT_BV':
                        scema='BIZ_VAULT'
                    mydata={'version': 2,
                            'sources':[{'name': scema,
                                       'tables':[]
                            }
                                       ]
                                       }
                    if sheet=='RV_TO_IM':
                        mydata['sources'][0]['database']=df_tgt_db_main
                    not_null_list=['not null','Not null', 'Not Null', 'NOT NULL','not Null','Y','NOT NULL ']
                    filtered_df=df_info[df_info['Not_null/Null'].isin(not_null_list)]
                    not_null_column=filtered_df.groupby('Target: Table/API')['field_name'].agg(list).to_dict()
                    k=list(dirt.keys())
                    #print(dirt)
                    for table in dirt:
                        table_name= table.split('.')[-1] if '.' in table else table
                        table_name=table_name.strip()
                        new_table_data={'name':f'{table_name}','tags':[f'{table_name}'],
                        'tests':[]}
                        
                        if table in df_dup_main:
                            dup_check={'dbt_utils.unique_combination_of_columns':{'combination_of_columns':list(set(df_dup_main[table])),'name':f'{table}-DUPLICATE CHECK FOR KEY COLUMNS','tags':['Key_Duplicate',f'{table}_Key_Duplicate']}}
                            new_table_data['tests'].append(dup_check)
                        if table in not_null_column:
                            not_null_test={'dbt_expectations.expect_column_values_to_not_be_null':{'column_name':list(set(not_null_column[table])),'name':f'{table}-NOT NULL CHECK','tags':['Not_Null',f'{table}_Not_Null']}}
                            new_table_data['tests'].append(not_null_test)
                        
                        #data_val
                        formated1=','.join(list('|'.join(str(elm) if elm is not None and not (isinstance(elm,float) and np.isnan(elm)) else '' for elm in i) for i in zipped_data[table]))
                        
                        rows=formated1.split(',')
                        data=[row.split('|') for row in rows]
                        df_new=pd.DataFrame(data)
                        df_new.columns=['tgt_fields','src_table','src_fields']
                        df_new_filtered=df_new[df_new['src_fields'].notnull() &(df_new['src_fields']!='')]
                        #print(df_new_filtered)
                        df_new_filtered['tgt_fields']=df_new_filtered['tgt_fields'].str.strip()
                        df_new_filtered['src_fields']=df_new_filtered['src_fields'].str.strip()
                        target_field_list=df_new_filtered['tgt_fields'].tolist()
                        src_field_list=df_new_filtered['src_fields'].tolist()
                        #print(df_new_filtered['src_table'][0])
                        dup_all_col=list(set(dirt[table]))
                        if 'LOAD_TS'in dup_all_col:
                            dup_all_col.remove('LOAD_TS')
                        dup_all={'dbt_utils.unique_combination_of_columns':{'combination_of_columns':dup_all_col,'name':f'{table}-DUPLICATE CHECK FOR ALL COLUMNS','tags':['All_Column_Duplicate',f'{table}_All_Column']}}
                        new_table_data['tests'].append(dup_all)
                        if not df_new_filtered.empty:
                            if sheet=='STAGE_RAWVAULT':
                                df_src_schema=df_info[df_info['Source Object'].notnull() &(df_info['Source Object']!='')]['Source Object'][0]
                                #print(df_src_schema)
                                df_src_schema=df_src_schema.replace('PRD1','QA1')
                                tab=df_new_filtered['src_table'].dropna().iloc[0]
                                source_table=f"{df_src_schema}.{tab}"
                                data_val_test={'Data_gen':{'compare_table':source_table,'column_name_src':','.join(src_field_list),'column_name_tgt':','.join(target_field_list),'name':f'{table}-DATA VALIDATION','tags':['Data_Validation',f'{table}_Data_Validation'] }}
                                
                                new_table_data['tests'].append(data_val_test)
                                count_check={'count_check':{'compare_table':source_table,'name':f'{table}-COUNT VALIDATION','tags':['Count_Validation',f'{table}_Count_Validation']}}
                                new_table_data['tests'].append(count_check)
                        
                        formated=','.join(list('|'.join(str(elm) if elm is not None and not (isinstance(elm,float) and np.isnan(elm)) else '' for elm in i) for i in zepped[table]))
                        rows_ddl=formated.split(',')
                        condition_default=[]
                        if table in default_fin:
                            formated_default=','.join(list('|'.join(str(elm) if elm is not None and not (isinstance(elm,float) and np.isnan(elm)) else '' for elm in i) for i in default_fin[table] ))
                            rows_default=formated_default.split(',')
                            data_default=[row.split('|') for row in rows_default]
                            df_default=pd.DataFrame(data_default)
                            df_default.columns=['Field_names','default']
                            
                            for d_inx,d_r in df_default.iterrows():
                                cond_def=f"{d_r['Field_names'].strip()}<>'{d_r['default'].strip()}'" if d_r['default'].strip().upper()!='NULL' else f"{d_r['Field_names'].strip()}<>{d_r['default'].strip()}" 
                                condition_default.append(cond_def)
                            where_default=' AND '.join(condition_default)
                        #print(df_default)
                        data_ddl=[row.split('|') for row in rows_ddl]
                        df_ddl=pd.DataFrame(data_ddl)
                        df_ddl.columns=['Field_names','Data_type']
                        df_ddl=df_ddl.drop_duplicates(keep='first')
                        #print(df_ddl)
                        df_ddl['Data_type']=df_ddl['Data_type'].dropna().apply(lambda x:re.sub(r'\(.*?\)','',x))
                        condition=[]
                        column_name=[]
                        #print(where_default)
                        if condition_default:
                            default_check={'Default_check':{'where_cond':where_default,'name':f'{table}-DEFAULT CHECK','tags':['Default_Validation',f'{table}_Default_Validation']}}
                            new_table_data['tests'].append(default_check)
                        for index, row in df_ddl.iterrows():
                            cond=f"(COLUMN_NAME='{row['Field_names'].strip()}' and DATA_TYPE<>'{row['Data_type'].strip()}')"
                            condition.append(cond)
                            column_name_1=f"{row['Field_names'].strip()}"
                            column_name.append(column_name_1)
                            
                        where_main='OR '.join(condition)
                        where_main=where_main.upper()
                        where_main_rep=where_main.replace('VARCHAR','TEXT')
                        if sheet=='RV_TO_IM':
                            where_part_1=f"WHERE TABLE_NAME = '{table_name}'"
                        else:
                            where_part_1=f"WHERE TABLE_NAME = '{table_name}'"
                        column_name_main=tuple(column_name)
                        #print(column_name_main)
                        where_final=f"WHERE ({where_main_rep})"
                        ddl_key='DDL'
                        if sheet=='RV_TO_IM':
                            ddl_key='DDL_IM'
                            ddl_check={ddl_key:{'where_con_part_1':where_part_1,'where_con_part_2':f"{column_name_main}",'db_name':df_tgt_db_main,
                                              'where_con_part_3':where_final,'name':f'{table}-DDL VALIDATION','tags':['DDL_Validation',f'{table}_DDL_Validation']}}
                        else:
                            ddl_check={ddl_key:{'where_con_part_1':where_part_1,'where_con_part_2':f"{column_name_main}",
                                              'where_con_part_3':where_final,'name':f'{table}-DDL VALIDATION','tags':['DDL_Validation',f'{table}_DDL_Validation']}}
                        new_table_data['tests'].append(ddl_check)
                        mydata['sources'][0]['tables'].append(new_table_data)
                    with open(f"{path_1}//models//{files.split('.')[0]}_{sheet}.yaml",'w') as f:
                       yaml.dump(mydata,f,default_flow_style=False)
                       print(f"Done {files.split('.')[0]}")
                except ValueError:
                    print('NO {sheet} found')