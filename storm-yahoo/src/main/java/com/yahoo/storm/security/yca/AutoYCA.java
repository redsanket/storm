package com.yahoo.storm.security.yca;

import backtype.storm.security.auth.IAutoCredentials;
import backtype.storm.utils.ShellUtils.ShellCommandExecutor;
import backtype.storm.utils.ShellUtils.ExitCodeException;

import java.util.concurrent.ConcurrentHashMap;
import java.util.Collection;
import java.util.Map;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.UUID;
import java.net.URLEncoder;

import java.io.InputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.File;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.ParserConfigurationException;
import javax.security.auth.Subject;

import yjava.security.yca.CertDatabase;
import yjava.security.yca.YCAException;

import org.apache.storm.commons.io.FileUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.DOMException;
import org.xml.sax.SAXException;

/**
 * Automatically push YCA certificates to worker processes. Also provides APIs
 * for bolts and spouts to fetch the certs.
 */
public class AutoYCA implements IAutoCredentials {
    private static final Logger LOG = LoggerFactory.getLogger(AutoYCA.class);
    private static final ConcurrentHashMap<String, String> V2_CACHE = new ConcurrentHashMap<String, String>();
    private static final String YCA_YAHOO_PREFIX = "yahoo.";
    static final String YCA_CRED_PREFIX = "ycav2-cert:";
    /**Config to set to a comma separated list of YCAv2 app IDs to push to workers*/
    public static final String YCA_APPIDS_CONF = "yahoo.autoyca.appids";
    /**
     *  Config to set to a YCAv1 app id that is used to fetch V2 app ids.
     * If not set kerberos is used to fetch the v2 certs.
     */
    public static final String YCA_V1_APPID_CONF = "yahoo.autoyca.v1appid";
    /**
     *  The proxy role to request when getting V2 certs.
     * If not provided a best effort will be done to get the right IP addresses in the certs.
     */
    public static final String YCA_PROXY_APPID_CONF = "yahoo.autoyca.proxyappid";
    private static CertDatabase cdb = null;
    private Map conf;

    private static String coanonicalAppId(String appId) {
        if (appId != null && !appId.startsWith(YCA_YAHOO_PREFIX)) {
            //V1 API needs the yahoo.
            appId = YCA_YAHOO_PREFIX + appId;
        }
        return appId;
    }

    private static String appIdForWebService(String appId) {
        if (appId == null) {
            return null;
        }
        if (appId.startsWith(YCA_YAHOO_PREFIX)) {
            appId = appId.substring(YCA_YAHOO_PREFIX.length());
        }
        try {
            return URLEncoder.encode(appId,"UTF-8");
        } catch (IOException e) {
            //This should never happen, but just in case
            throw new RuntimeException(e);
        }
    }

    /**
     * Get a YCA v1 cert from the certDB.
     * @param appId the application id of the cert you would like to request.
     * @return the body of the cert or null, if no cert could be found.
     * @throws YCAException if an error occurs trying to get a cert.
     */ 
    public static synchronized String getYcaV1Cert(String appId) throws YCAException {
        String cert = null;
        if (appId != null) {
            if (cdb == null) {
                cdb = new CertDatabase();
            }

            cert = cdb.getCert(coanonicalAppId(appId));
        }
        return cert;
    }

    /**
     * Get a YCA v2 cert from the cache.
     * NOTE: that you must go through an http proxy that this cert was configured to use. 
     * @param appId the application id of the cert you would like to request.
     * @return the body of the cert or null, if no cert could be found.
     * @throws YCAException if an error occurs trying to get a cert.
     */ 
    public static String getYcaV2Cert(String appId) throws YCAException {
        return V2_CACHE.get(coanonicalAppId(appId));
    }

    /**
     * Get a YCA cert to use.
     * Will first try to get a cached YCAv2 cert, if no v2 cert could be found, will try
     * to get a v1 cert from the current host. NOTE that if you get a V2Cert it will only
     * work going through an http proxy server that it has been configured for.  This is
     * a convinence method to make it simpler for code to run in both a hosted and non-hosted
     * environment.  If you only intend to run on multi-tenant storm, please use getYcaV2Cert
     * directly.
     * @param appId the application id of the cert you would like to request.
     * @return the body of the cert or null, if no cert could be found.
     * @throws YCAException if an error occurs trying to get a cert.
     */ 
    public static String getYcaCert(String appId) throws YCAException {
        String ret = getYcaV2Cert(appId);
        if (ret == null) {
          ret = getYcaV1Cert(appId);
        }
        return ret;
    }

    /**
     * Remove all entries from the YCA V2 cache.
     * This is intended to really only be used for testing purposes,
     * because you do not want to get rid of your certificates in production.
     */ 
    public static void clearV2Cache() {
        V2_CACHE.clear();
    }

    /**
     * Set a v2 cert in the cache.
     * This is really only intended to be used for testing, because the certs should
     * be pushed automatically to the workers for you.
     * @param appid the application id/role of the cert
     * @param the body of the cert
     */ 
    public static void setV2Cert(String appid, String cert) {
        V2_CACHE.put(coanonicalAppId(appid), cert);
    }

    /**
     * Parse the YCA certs out of the credentials
     * @param credentials the credentials that need to be parsed
     * @param ycaCerts a map of app id to cred that will be updated with new certs after processing.
     */ 
    public static void updateV2CertsFromCreds(Map<String, String> credentials, Map<String, String> ycaCerts) {
        if (credentials == null) {
            LOG.warn("Null credentials passed in to updateV2CertsFromCreds, ignoring.");
            return;
        }
        for (Map.Entry<String,String> entry: credentials.entrySet()) {
            if (entry.getKey().startsWith(YCA_CRED_PREFIX)) {
                String appId = coanonicalAppId(entry.getKey().substring(YCA_CRED_PREFIX.length()));
                String cert = entry.getValue();
                LOG.info("Adding cert for {}", appId);
                ycaCerts.put(appId, cert); 
            }
        }
    }

    /* Temporarily workaround an issue with jdk8 and empty REALM in the ticket cache */
    private static File copyKrbFile() throws IOException {
        File tmpFile = new File("/tmp/" + UUID.randomUUID().toString());
        ShellCommandExecutor shExec;
        List<String> command = new ArrayList<String>(Arrays.asList("klist", "-l"));
        String[] commandArray = command.toArray(new String[command.size()]);
        shExec = new ShellCommandExecutor(commandArray);
        try {
            shExec.execute();
            LOG.debug("output: {}", shExec.getOutput());
            Pattern filePattern= Pattern.compile(".* FILE:(.+)$");
            Matcher matcher = filePattern.matcher(shExec.getOutput());
            if (matcher.find()) {
                String krbFile = matcher.group(1);
                LOG.debug("krb5 file is: " + krbFile);
                // use the os cp to preserve permissions
                List<String> cpcommand = new ArrayList<String>(Arrays.asList("cp", krbFile, tmpFile.getCanonicalPath()));
                String[] cpcommandArray = cpcommand.toArray(new String[cpcommand.size()]);
                ShellCommandExecutor cpshExec = new ShellCommandExecutor(cpcommandArray);
                LOG.info("copying krb5 file");
                try {
                    cpshExec.execute();
                    LOG.debug("krb5 copied to: {} output: {}", tmpFile, cpshExec.getOutput());
                } catch (ExitCodeException e) {
                    int exitCode = cpshExec.getExitCode();
                    LOG.error("Exit code from copy command is: " + exitCode, e);
                    LOG.debug("output: {}", cpshExec.getOutput());
                    try {
                        FileUtils.forceDelete(tmpFile);
                    } catch (IOException ignore) {}
                    return null;
                }
            } else {
                LOG.error("could not find kerberos ticket cache file - did you kinit?");
            }
        } catch (ExitCodeException e) {
            int exitCode = shExec.getExitCode();
            LOG.error("Error doing klist, did you kinit? Exit code is : " + exitCode, e);
            LOG.debug("output: {}", shExec.getOutput());
            return null;
        }
        return tmpFile;
    }

    private static String getYcaV2CertRawWithRetry(String v2Role, String v1Role, String proxyRole) throws YCAException, InterruptedException {
       try {
          return getYcaV2CertRaw(v2Role, v1Role, proxyRole);
       } catch(Exception e) {
          LOG.error("First try ERROR:", e); 
       }
       Thread.sleep(1000);
       try {
          return getYcaV2CertRaw(v2Role, v1Role, proxyRole);
       } catch(Exception e) {
          LOG.error("Second try ERROR:", e); 
       }
       Thread.sleep(3000);
       return getYcaV2CertRaw(v2Role, v1Role, proxyRole);
    }

    private static String getYcaV2CertRaw(String v2Role, String v1Role, String proxyRole) throws YCAException {
        v2Role = coanonicalAppId(v2Role);
        String v2RoleForWeb = appIdForWebService(v2Role);
        String proxyRoleForWeb = appIdForWebService(proxyRole);

        String v1Cert = null;
        if (v1Role != null) {
            v1Cert = getYcaV1Cert(v1Role);
            if (v1Cert == null) {
                throw new YCAException("The current box does not appear to be part of "+v1Role);
            }
        }

        File krbFile = null;
        ByteArrayOutputStream outTmp = new ByteArrayOutputStream();
        final ByteArrayOutputStream errTmp = new ByteArrayOutputStream();
        try {
            ArrayList<String> cmd = new ArrayList<String>();
            cmd.add("curl");
            cmd.add("-s");
            cmd.add("-S");
            String proxyPart = (proxyRoleForWeb == null) ? "" : ("?http_proxy_role="+proxyRoleForWeb);

            if (v1Cert != null) {
                cmd.add("http://ca.yca.platform.yahoo.com:4080/wsca/v2/certificates/yca/"+v2RoleForWeb+proxyPart);
                cmd.add("-vvvv");
                cmd.add("-H");
                cmd.add("Yahoo-App-Auth:"+v1Cert);
            } else { //user kerberos
                cmd.add("-vvvv");
                cmd.add("--negotiate");
                cmd.add("-u");
                cmd.add(":");
                cmd.add("http://ca.yca.platform.yahoo.com:4080/wsca/v2/certificates/kerberos/"+v2RoleForWeb+proxyPart);

                krbFile = copyKrbFile();
            }
            LOG.info("Running {} to get YCAv2 Cert",cmd);
            ProcessBuilder pb = new ProcessBuilder(cmd);
            if (krbFile != null) {
              Map<String, String> env = pb.environment();
              env.put("KRB5CCNAME", krbFile.getCanonicalPath());
              LOG.info("Will use different krb file {}",krbFile.getCanonicalPath());
            }
            Process p = pb.start();
            InputStream in = p.getInputStream();
            final InputStream err = p.getErrorStream();
            Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        byte [] buffer = new byte[1024];
                        int read = 0;
                        while ((read = err.read(buffer)) >= 0) {
                            errTmp.write(buffer,0,read);
                        }
                    } catch (IOException e) {
                        LOG.error("ERROR", e);
                        throw new RuntimeException(e);
                    }
                }
            });
            t.start();
            byte [] buffer = new byte[1024];
            int read = 0;
            while ((read = in.read(buffer)) >= 0) {
                outTmp.write(buffer,0,read);
            }
            int ret = p.waitFor();
            t.join();
            if (ret != 0) {
                LOG.error("result of command: {}", new String(outTmp.toByteArray()));
                LOG.error("std err of the command {}", new String(errTmp.toByteArray()));
                throw new YCAException("Something went wrong and "+cmd+" exited with "+ret+" please check the logs for more details");
            }
            DocumentBuilderFactory builderFactory =
                DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = builderFactory.newDocumentBuilder();
            Document document = builder.parse(new ByteArrayInputStream(outTmp.toByteArray()));
            Node n = document.getDocumentElement().getElementsByTagName("certificate").item(0);
            if (n == null) {
                LOG.error("result of command: {}", new String(outTmp.toByteArray()));
                LOG.error("std err of the command {}", new String(errTmp.toByteArray()));
                throw new YCAException("Could not find the certificate in returned XML.");
            }
            return n.getTextContent();
        } catch (SAXException | DOMException | ParserConfigurationException e) {
            LOG.error("result of command: {}", new String(outTmp.toByteArray()));
            LOG.error("std err of the command {}", new String(errTmp.toByteArray()));
            throw new YCAException("Error parsing YCA curl result ", e);
        } catch (IOException | InterruptedException e2) {
            LOG.error("result of command: {}", new String(outTmp.toByteArray()));
            LOG.error("std err of the command {}", new String(errTmp.toByteArray()));
            throw new YCAException("Error running YCA curl command ", e2);
        } finally {
            if (krbFile != null) {
                try {
                    FileUtils.forceDelete(krbFile);
                } catch(IOException ignore) {
                    LOG.warn("error removing: " + krbFile.toString());
                }
            }
        }
    }

    @Override
    public void prepare(Map conf) {
        this.conf = conf;
    }

    @Override
    public void populateCredentials(Map<String, String> credentials) {
        LOG.info("Populating credentials with YCA certs");
        Object v2Conf = conf.get(YCA_APPIDS_CONF);
        if (v2Conf == null) {
            LOG.info("No certs requested, if you want to use YCAv2 certs please set {} to the list of certs to request", YCA_APPIDS_CONF);
            return;
        }
        Collection<String> v2AppIds = null;
        if (v2Conf instanceof Collection) {
            v2AppIds = (Collection<String>)v2Conf;
        } else if (v2Conf instanceof String) {
            v2AppIds = Arrays.asList(((String)v2Conf).split(","));
        } else {
            throw new RuntimeException(YCA_APPIDS_CONF+" is not set to something that I know how to use "+v2Conf);
        }

        String v1AppId = (String)conf.get(YCA_V1_APPID_CONF);
        String proxyAppId = (String)conf.get(YCA_PROXY_APPID_CONF);
        try {
            for (String appId: v2AppIds) {
                appId = coanonicalAppId(appId);
                String cert = getYcaV2CertRawWithRetry(appId, v1AppId, proxyAppId);
                credentials.put(YCA_CRED_PREFIX + appId, cert);
            }
        } catch (YCAException|InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void updateSubject(Subject subject, Map<String, String> credentials) {
        populateSubject(subject, credentials);
    }

    @Override
    public void populateSubject(Subject subject, Map<String, String> credentials) {
        LOG.info("Populating YCA certs from credentials");
        //ignore the subject and just pull out the YCA Certs into the cache
        updateV2CertsFromCreds(credentials, V2_CACHE);
    }


    public static void main(String[] args) throws Exception {
        for (String s: args) {
            System.out.println(s+" : "+getYcaV2CertRawWithRetry(s, null, null));
        }
    }
}
