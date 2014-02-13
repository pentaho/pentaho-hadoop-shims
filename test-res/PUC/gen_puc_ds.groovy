import groovy.xml.MarkupBuilder
import java.util.zip.ZipOutputStream  
import java.util.zip.ZipEntry  

// Read config from PDI's properties
def p = System.getProperties()
p.load(new BufferedReader(new FileReader('../PDI/SuperJobParameters.properties')))

def datasources = [

jdbc_hive1:  [
                host: p.get('Hive1Host','localhost'),
                db: 'default',
                port: p.get('Hive1Port','10000'),
                type: 'Hadoop Hive',
                shortName: 'HIVE',
                //user: 'username'
                //password: 'password'
             ],
jdbc_hive2:  [
                host: p.get('Hive2Host','localhost'),
                db: 'default',
                port: p.get('Hive2Port', '10000'),
                defaultPort: '10000',
                type: 'Hadoop Hive 2',
                shortName: 'HIVE2',
                //user: 'username',
                //password: 'password',
             ],

jdbc_impala: [
                host: p.get('ImpalaHost','localhost'),
                db: 'default',
                port: p.get('ImpalaPort','21050'),
                defaultPort: '21050',
                type: 'Impala',
                shortName: 'IMPALA',
                //user: 'username',
                //password: 'password',
             ],    
]

// Take the datasources map above and generate an export manifest, 
// then ZIP it up for ingest by BA server
def exportManifestFile = new File('exportManifest.xml')
def zipFileName = 'puc_hive_datasources.zip'
def writer = new StringWriter()
def xml = new MarkupBuilder(writer)
xml.doubleQuotes = true
xml.mkp.xmlDeclaration(version: '1.0', encoding: 'UTF-8', standalone: 'yes')

xml.'ns2:ExportManifest'('xmlns:ns2':'http://www.pentaho.com/schema/') {
  ExportManifestInformation(exportBy: 'admin', rootFolder: '/')
  ExportManifestProperty {
      EntityMetaData(name:'public', isFolder:true, path:'public', isHidden:false, locale:'en', title:'public')
  }
  
  datasources.each { k,v ->
    ExportManifestDatasource {
        name(k)
        accessType('NATIVE')
        accessTypeValue('NATIVE')
        attributes()
        changed(true)
        connectionPoolingProperties()
        databaseName(v.db)
        databasePort(v.port)
        databaseType {
            defaultDatabasePort(v?.defaultPort ?: v.port)
            name(v.type)
            shortName(v.shortName)
        }
        extraOptions()
        forcingIdentifiersToLowerCase(false)
        forcingIdentifiersToUpperCase(false)
        hostname(v.host)
        initialPoolSize(0)
        maximumPoolSize(20)
        partitioned(false)
        password(v?.password)
        quoteAllFields(false)
        streamingResults(false)
        username(v?.user)
        usingConnectionPool(false)
        usingDoubleDecimalAsSchemaTableSeparator(false)
    }
  }
  
  ExportManifestMondrian(file: 'etc/models/hive1-1table.mondrian.xml') {
      catalogName('hive1-1table')
      xmlaEnabled(false)
      parameters {
          entries {
              entry(key:'Provider', value:'mondrian')
              entry(key:'DataSource', value:'jdbc_hive1')
              entry(key:'EnableXmla', value:false)
          }
      }
  }
  
  ExportManifestMondrian(file: 'etc/models/hive1-sqlstar.mondrian.xml') {
      catalogName('hive1-sqlstar')
      xmlaEnabled(false)
      parameters {
          entries {
              entry(key:'Provider', value:'mondrian')
              entry(key:'DataSource', value:'jdbc_hive1')
              entry(key:'EnableXmla', value:false)
          }
      }
  }
  
  ExportManifestMondrian(file: 'etc/models/hive2-1table.mondrian.xml') {
      catalogName('hive2-1table')
      xmlaEnabled(false)
      parameters {
          entries {
              entry(key:'Provider', value:'mondrian')
              entry(key:'DataSource', value:'jdbc_hive2')
              entry(key:'EnableXmla', value:false)
          }
      }
  }
  
  ExportManifestMondrian(file: 'etc/models/hive2-sqlstar.mondrian.xml') {
      catalogName('hive2-sqlstar')
      xmlaEnabled(false)
      parameters {
          entries {
              entry(key:'Provider', value:'mondrian')
              entry(key:'DataSource', value:'jdbc_hive2')
              entry(key:'EnableXmla', value:false)
          }
      }
  }
  
  ExportManifestMetadata(domainId:'hive1-sqlstar', file:'etc/models/hive1-sqlstar.xmi')
  ExportManifestMetadata(domainId:'hive1-1table',  file:'etc/models/hive1-1table.xmi')
  ExportManifestMetadata(domainId:'hive2-sqlstar', file:'etc/models/hive2-sqlstar.xmi')
  ExportManifestMetadata(domainId:'hive2-1table',  file:'etc/models/hive2-1table.xmi')


  // Add reports to manifest
  new File('public').eachFileMatch(~/.*\.xanalyzer/) { file ->
    def fileName = file.name
    // Strip extension
    def shortName = fileName.lastIndexOf('.') >= 0 ? fileName[0..(fileName.lastIndexOf('.')-1)] : fileName
    
    ExportManifestEntity(path:"public/$fileName") {
      ExportManifestProperty {
          EntityMetaData(name: "$fileName", 
                         //createdDate: '2014-02-12T12:56:45.294-05:00', 
                         isFolder: false, 
                         path: "public/$fileName", 
                         isHidden: false,
                         locale: 'en_US',
                         title: "$shortName")
      }
      ExportManifestProperty {
          EntityAcl {
              entriesInheriting(true)
              owner('admin')
              ownerType('USER')
          }
      }
    }
  }
}

// Generate and add export manifest to ZIP
String text = writer.toString()
exportManifestFile.withWriter{ out -> out.writeLine(text) }

ZipOutputStream zipFile = new ZipOutputStream(new FileOutputStream(zipFileName)) 

// Build ZIP file
new File('.').eachFileRecurse { file ->
  def filename = file.path
  if(!file.isDirectory() && !filename.endsWith('.groovy') && !filename.endsWith('.zip')) {
    ZipEntry entry = new ZipEntry(filename)
    entry.time = file.lastModified()
    zipFile.putNextEntry(entry)
    zipFile << new FileInputStream(file)
  }
}

// Cleanup artifacts (close ZIP, delete temp files)
zipFile.close()
exportManifestFile.delete()