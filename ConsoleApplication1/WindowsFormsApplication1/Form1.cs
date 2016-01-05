using ConsoleApplication1;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace WindowsFormsApplication1
{
    public partial class Form1 : Form
    {
        public Form1()
        {
            InitializeComponent();
        }

        private void Form1_Load(object sender, EventArgs e)
        {
            
            
            acessoLinqTabelas obj = new acessoLinqTabelas();
            DataTable dt = new DataTable();
            dt = Utils.ConvertListToDataTable(obj.getConveniados(174244));
            dataGridView1.DataSource = dt;
            //dt = obj.getConveniados(174244);
            //dataGridView1.DataSource = dt;
        }
    }
}
