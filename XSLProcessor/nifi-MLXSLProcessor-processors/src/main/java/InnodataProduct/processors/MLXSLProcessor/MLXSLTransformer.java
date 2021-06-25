package InnodataProduct.processors.MLXSLProcessor;

import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.ErrorListener;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Templates;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.BufferedInputStream;
import org.apache.nifi.util.StopWatch;
import org.apache.nifi.util.Tuple;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;

import net.sf.saxon.Controller;
import net.sf.saxon.TransformerFactoryImpl;
import net.sf.saxon.event.MessageWarner;
import net.sf.saxon.trans.XPathException;



//import com.google.common.cache.CacheBuilder;
//import com.google.common.cache.CacheLoader;
//import com.google.common.cache.LoadingCache;

@EventDriven
@SideEffectFree
@SupportsBatching
@Tags({"xml", "xslt", "transform"})
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Applies the provided XSLT file to the flowfile XML payload. A new FlowFile is created "
        + "with transformed content and is routed to the 'success' relationship. If the XSL transform "
        + "fails, the original FlowFile is routed to the 'failure' relationship")
@DynamicProperty(name = "An XSLT transform parameter name", value = "An XSLT transform parameter value", supportsExpressionLanguage = true,
        description = "These XSLT parameters are passed to the transformer")
public class MLXSLTransformer extends AbstractProcessor {

    public static final PropertyDescriptor XSLT_FILE_NAME = new PropertyDescriptor.Builder()
            .name("XSLT file name")
            .description("Provides the name (including full path) of the XSLT file to apply to the flowfile XML content.")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .build();
    
    public static final PropertyDescriptor LOG_FILE = new PropertyDescriptor.Builder()
            .name("Log file path")
            .description("Provides the name (including full path) of the Log file for the logging.")
            .required(false)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.ATTRIBUTE_EXPRESSION_LANGUAGE_VALIDATOR)
            .build();
    
    public static final PropertyDescriptor MORE_INPUT_FILE_NAME = new PropertyDescriptor.Builder()
    .name("Order metadata file")
    .description("Provides the name (including full path) of the additional files for processing.")
    .required(false)
    .expressionLanguageSupported(true)
    .addValidator(StandardValidators.ATTRIBUTE_EXPRESSION_LANGUAGE_VALIDATOR)
    .build();

    public static final PropertyDescriptor INDENT_OUTPUT = new PropertyDescriptor.Builder()
            .name("indent-output")
            .displayName("Indent")
            .description("Whether or not to indent the output.")
            .required(true)
            .defaultValue("true")
            .allowableValues("true", "false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final PropertyDescriptor SECURE_PROCESSING = new PropertyDescriptor.Builder()
            .name("secure-processing")
            .displayName("Secure processing")
            .description("Whether or not to mitigate various XML-related attacks like XXE (XML External Entity) attacks.")
            .required(true)
            .defaultValue("true")
            .allowableValues("true", "false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final PropertyDescriptor CACHE_SIZE = new PropertyDescriptor.Builder()
            .name("cache-size")
            .displayName("Cache size")
            .description("Maximum number of stylesheets to cache. Zero disables the cache.")
            .required(true)
            .defaultValue("10")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor CACHE_TTL_AFTER_LAST_ACCESS = new PropertyDescriptor.Builder()
            .name("cache-ttl-after-last-access")
            .displayName("Cache TTL after last access")
            .description("The cache TTL (time-to-live) or how long to keep stylesheets in the cache after last access.")
            .required(true)
            .defaultValue("60 secs")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("The FlowFile with transformed content will be routed to this relationship")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("If a FlowFile fails processing for any reason (for example, the FlowFile is not valid XML), it will be routed to this relationship")
            .build();

    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;
    //private LoadingCache<String, Templates> cache;
    
    
    Path log_file_path = null;
    String logFileName = "";
    
    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(XSLT_FILE_NAME);
        properties.add(LOG_FILE);
        properties.add(MORE_INPUT_FILE_NAME);
        
        properties.add(INDENT_OUTPUT);
        properties.add(SECURE_PROCESSING);
        properties.add(CACHE_SIZE);
        properties.add(CACHE_TTL_AFTER_LAST_ACCESS);
        this.properties = Collections.unmodifiableList(properties);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .expressionLanguageSupported(true)
                .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING, true))
                .required(false)
                .dynamic(true)
                .build();
    }

    private Templates newTemplates(ProcessContext context, String path) throws TransformerConfigurationException {
        final Boolean secureProcessing = context.getProperty(SECURE_PROCESSING).asBoolean();
        TransformerFactory factory = TransformerFactory.newInstance();

        if (secureProcessing) {
            factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
            // don't be overly DTD-unfriendly forcing http://apache.org/xml/features/disallow-doctype-decl
            factory.setFeature("http://saxon.sf.net/feature/parserFeature?uri=http://xml.org/sax/features/external-parameter-entities", false);
            factory.setFeature("http://saxon.sf.net/feature/parserFeature?uri=http://xml.org/sax/features/external-general-entities", false);
        }

        return factory.newTemplates(new StreamSource(path));
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
		/*
		 * final ComponentLog logger = getLogger(); final Integer cacheSize =
		 * context.getProperty(CACHE_SIZE).asInteger(); final Long cacheTTL =
		 * context.getProperty(CACHE_TTL_AFTER_LAST_ACCESS).asTimePeriod(TimeUnit.
		 * SECONDS);
		 * 
		 * if (cacheSize > 0) { CacheBuilder cacheBuilder =
		 * CacheBuilder.newBuilder().maximumSize(cacheSize); if (cacheTTL > 0) {
		 * cacheBuilder = cacheBuilder.expireAfterAccess(cacheTTL, TimeUnit.SECONDS); }
		 * 
		 * cache = cacheBuilder.build( new CacheLoader<String, Templates>() { public
		 * Templates load(String path) throws TransformerConfigurationException { return
		 * newTemplates(context, path); } }); } else { cache = null;
		 * logger.warn("Stylesheet cache disabled because cache size is set to 0"); }
		 */
    }
    
    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        final FlowFile original = session.get();
        if (original == null) {
            return;
        }
        

        final ComponentLog logger = getLogger();
        final StopWatch stopWatch = new StopWatch(true);
         
        final String xsltFileName = context.getProperty(XSLT_FILE_NAME)
            .evaluateAttributeExpressions(original)
            .getValue();
        
        //Creating logfile for xsl message
        FileWriter fw;
        PrintWriter pw;
        
        log_file_path = Paths.get(context.getProperty(LOG_FILE).evaluateAttributeExpressions(original).getValue());
        
        logger.info("log_file_path message" + log_file_path);
        
        File log_file = new File(context.getProperty(LOG_FILE).evaluateAttributeExpressions(original).getValue());
        
        
        //Order XML or Json File Path
        final String More_input_File = context.getProperty(MORE_INPUT_FILE_NAME).evaluateAttributeExpressions(original).getValue();
        
        
        final Boolean indentOutput = context.getProperty(INDENT_OUTPUT).asBoolean();
       
        try {
        	
        	if (!Files.exists(log_file_path)) {
        		Files.createFile(log_file_path);
        	}
        	
        	logger.info("log_file_path message" + log_file_path);
        	
        	fw = new FileWriter(log_file, true);
			pw = new PrintWriter(fw);
			
			pw.write("********Error Log ***********");
			pw.write("\n");
        	        	
            FlowFile transformed = session.write(original, new StreamCallback() {
                @Override
                public void process(final InputStream rawIn, final OutputStream out) throws IOException {
                    try (
                    		@SuppressWarnings("deprecation")
					final InputStream in = new BufferedInputStream(rawIn)) {
                        //final Templates templates;
						/*
						 * if (cache != null) { templates = cache.get(xsltFileName); } else { templates
						 * = newTemplates(context, xsltFileName); }
						 */

                        //final Transformer transformer = templates.newTransformer();
                    	
                    	TransformerFactoryImpl f = new net.sf.saxon.TransformerFactoryImpl();
                    	f.setAttribute("http://saxon.sf.net/feature/version-warning", Boolean.FALSE);
                    	
                    	Transformer transformer = f.newTransformer(new StreamSource(xsltFileName));
                    	transformer.setOutputProperty(OutputKeys.INDENT, (indentOutput ? "yes" : "no"));
                    	
                    	transformer.setErrorListener(new ErrorListener() {
                    		@Override
							public void warning(TransformerException exception) throws TransformerException {
                    			exception.getLocalizedMessage();
                    			pw.write(exception.getLocalizedMessage());
                    			pw.write("\n");
                     			
							}
							
							@Override
							public void fatalError(TransformerException exception) throws TransformerException {
								warning(exception);
								
							}
							
							@Override
							public void error(TransformerException exception) throws TransformerException {
								warning(exception);
								
							}
						});
                    	
                    	
                    	
                    	transformer.setParameter("ORDER_FILE_LOC", More_input_File);;
                    	
                    	//transformer.setParameter("ORDER_FILE_LOC", Order_file_path.toString());
                    	
                    	Properties props = new Properties();
                    	props.setProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
                    	//transformer.setErrorListener(new ErrorListener());
                    	MessageWarner saxonWarner = new MessageWarner();
						
						 try { 
							 saxonWarner.setOutputProperties(props);
							 saxonWarner.setOutputStream(System.err); 
						  	} 
						  catch (XPathException e2)
						 {
						  e2.printStackTrace(); 
						  }
						  //((Controller)transformer).setMessageEmitter(saxonWarner);
						 
                    	
                         Controller controller = (Controller) transformer;
                         //controller.setMessageEmitter(new MessageWarner());
                         controller.setMessageEmitter(saxonWarner);

                        // pass all dynamic properties to the transformer
                        for (final Map.Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
                            if (entry.getKey().isDynamic()) {
                                String value = context.newPropertyValue(entry.getValue()).evaluateAttributeExpressions(original).getValue();
                                transformer.setParameter(entry.getKey().getName(), value);
                            }
                        }
                        
                        // use a StreamSource with Saxon
                        StreamSource source = new StreamSource(in);
                        StreamResult result = new StreamResult(out);
                        transformer.transform(source, result);
                    } catch (final Exception e) {
                        throw new IOException(e);
                    }
                }
            });
            session.transfer(transformed, REL_SUCCESS);
            session.getProvenanceReporter().modifyContent(transformed, stopWatch.getElapsed(TimeUnit.MILLISECONDS));
            logger.info("Transformed {}", new Object[]{original});
            pw.write("Transformed {}" + new Object[]{original});
            pw.write("\n");
            
            pw.close();    
            
        } catch (Exception e) {
            logger.error("Unable to transform {} due to {}", new Object[]{original, e});
            session.transfer(original, REL_FAILURE);
        } 
        
    }

	/*
	 * @SuppressWarnings("unused") private static final class XsltValidator
	 * implements Validator {
	 * 
	 * private volatile Tuple<String, ValidationResult> cachedResult;
	 * 
	 * @Override public ValidationResult validate(final String subject, final String
	 * input, final ValidationContext validationContext) { final Tuple<String,
	 * ValidationResult> lastResult = this.cachedResult; if (lastResult != null &&
	 * lastResult.getKey().equals(input)) { return lastResult.getValue(); } else {
	 * String error = null; final File stylesheet = new File(input); final
	 * TransformerFactory tFactory = new net.sf.saxon.TransformerFactoryImpl();
	 * final StreamSource styleSource = new StreamSource(stylesheet);
	 * 
	 * try { tFactory.newTransformer(styleSource); } catch (final Exception e) {
	 * error = e.toString(); }
	 * 
	 * this.cachedResult = new Tuple<>(input, new ValidationResult.Builder()
	 * .input(input) .subject(subject) .valid(error == null) .explanation(error)
	 * .build()); return this.cachedResult.getValue(); } } }
	 */

}