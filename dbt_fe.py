import tkinter as tk
from tkinter import filedialog, messagebox
#from filde_load_main_code import main
#from fixedwidth_to_df import fixed_width_main
from tkinter import simpledialog
from dbt_main import main
#from dq_automation import dq_validation
#from dq_first import form
def browse_source_path():
    source_path=filedialog.askdirectory()
    source_path_var.set(source_path)
def execute_program(source_path):
    source_path1=source_path.replace("\\","/")
    main(source_path1)
def create_config(source_path):
    source_path1=source_path.replace("\\","/")
    form(source_path1)
def convert_button():
    source_path=source_path_var.get()
    #create_config(source_path)
    #messagebox.showinfo("Note","Config files are created, please give the necessary details and click ok to continue")
    execute_program(source_path)
    messagebox.showinfo("Sucess","YAML generation is done")
    root.destroy()
root=tk.Tk()
root.title("DBT YAML Automation")


convert_button=tk.Button(root,text='Generate',command=convert_button)
source_path_var=tk.StringVar()
source_lable=tk.Label(root,text='Path:')
source_entry=tk.Entry(root,textvariable=source_path_var)
source_button=tk.Button(root,text='Browse',command=browse_source_path)
source_button.grid(row=0,column=2,padx=10,pady=5)
source_lable.grid(row=0,column=0,padx=10,pady=5,sticky=tk.E)
source_entry.grid(row=0,column=1,padx=10,pady=5,sticky=tk.W)
convert_button.grid(row=4,column=0,columnspan=3,pady=10)

root.mainloop()