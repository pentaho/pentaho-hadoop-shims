// Read config from PDI's properties
def p = System.getProperties()
p.load(new BufferedReader(new FileReader('../PDI/SuperJobParameters.properties')))

String outFilename = 'Inventory_model_HiveImpala.xmi'
String filename = "${outFilename}.template"

def file = new File(filename)
def contents = file.text

def outFile = new File(outFilename) 
outFile.delete()  
outFile << contents \
    .replaceAll(~/\$\{Hive1Host\}/, p.get('Hive1Host','my.hostname.com')) \
    .replaceAll(~/\$\{Hive1Port\}/, p.get('Hive1Port','10000')) \
    .replaceAll(~/\$\{Hive2Host\}/, p.get('Hive2Host','my.hostname.com')) \
    .replaceAll(~/\$\{Hive2Port\}/, p.get('Hive2Port','10000')) \
    .replaceAll(~/\$\{ImpalaHost\}/, p.get('ImpalaHost','my.hostname.com')) \
    .replaceAll(~/\$\{ImpalaPort\}/, p.get('ImpalaPort','21050'))
