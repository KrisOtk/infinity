package com.zwq.infinity.plugin.filters;

import com.alibaba.fastjson.JSON;
import com.zwq.infinity.baseplugin.BaseFilter;
import com.zwq.infinity.baseplugin.BaseMetric;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.jcodings.specific.UTF8Encoding;
import org.joni.*;

import java.io.*;
import java.net.JarURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

/**
 * 根据匹配规则对数据进行处理的filter
 */
@Slf4j
public class Grok extends BaseFilter {

    private static final String PATH = "patterns/";
    private static final Long MAX_LENGTH = 300000L;

    private String src;
    private String encoding;
    private Long lengthLimit = MAX_LENGTH;
    private List<Regex> matches;
    private Map<String, String> patterns;



    private final AtomicLong grokCount = new AtomicLong(0);
    private final ThreadLocal<Long> threadLocal = ThreadLocal.withInitial(() -> 0L);


    public Grok(Map<String, Object> config) {
        super(config);
    }
    public Grok(Map<String, Object> config, List<BaseMetric> metrics) {
        super(config, metrics);
    }

    /**
     * <p>核心方法 转换grok规则</p>
     *
     * @param p
     * @return
     */
    private String convertPatternOneLevel(String p) {
        String pattern = "%\\{[_0-9a-zA-Z]+(:[-_.0-9a-zA-Z]+){0,2}}";
        java.util.regex.Matcher m = java.util.regex.Pattern.compile(pattern).matcher(p);
        StringBuilder newPattern = new StringBuilder();
        int lastEnd = 0;
        while (m.find()) {
            newPattern.append(p, lastEnd, m.start());
            String syntaxAndSemantic = m.group(0).substring(2, m.group(0).length() - 1);
            String syntax, semantic;
            String[] syntaxAndSemanticArray = syntaxAndSemantic.split(":", 3);
            syntax = syntaxAndSemanticArray[0];
            if (syntaxAndSemanticArray.length > 1) {
                semantic = syntaxAndSemanticArray[1];
                newPattern.append("(?<").append(semantic).append(">").append(patterns.get(syntax)).append(")");
            } else {
                newPattern.append(patterns.get(syntax));
            }
            lastEnd = m.end();
        }
        newPattern.append(p.substring(lastEnd));
        return newPattern.toString();
    }

    private String convertPattern(String matchString) {
        do {
            String rst = this.convertPatternOneLevel(matchString);
            if (rst.equals(matchString)) {
                return matchString;
            }
            matchString = rst;
        } while (true);
    }

    /**
     * <p>读取目录下的文件</p>
     * <p>key value 读取</p>
     *
     * @param path
     */
    private void loadPatterns(File path) {
        if (path.isDirectory()) {
            for (File subpath : Objects.requireNonNull(path.listFiles())) {
                loadPatterns(subpath);
            }
        } else {
            FileReader in = null;
            BufferedReader br = null;
            try {
                in = new FileReader(path);
                br = new BufferedReader(in);
                String currentLine;
                while ((currentLine = br.readLine()) != null) {
                    currentLine = currentLine.trim();
                    if (currentLine.length() == 0 || currentLine.indexOf("#") == 0) {
                        continue;
                    }
                    this.patterns.put(currentLine.split("\\s", 2)[0], currentLine.split("\\s", 2)[1]);
                }
            } catch (IOException e) {
                log.info("grok读取目录下的正则规则失败:{}", ExceptionUtils.getStackTrace(e));
            } finally {
                try {
                    if (in != null) {
                        in.close();
                    }
                    if (br != null) {
                        br.close();
                    }
                } catch (IOException e) {
                    log.info("FileReader or BufferedReader close error:{}", ExceptionUtils.getStackTrace(e));
                }

            }
        }

    }

    private void loadPatterns(InputStream resourceAsStream) {
        BufferedReader br = null;
        try {
            br = new BufferedReader(new InputStreamReader(resourceAsStream));
            String currentLine;
            while ((currentLine = br.readLine()) != null) {
                currentLine = currentLine.trim();
                if (currentLine.length() == 0 || currentLine.indexOf("#") == 0) {
                    continue;
                }
                this.patterns.put(currentLine.split("\\s", 2)[0], currentLine.split("\\s", 2)[1]);
            }
        } catch (IOException e) {
            log.info("load patterns error:{}", ExceptionUtils.getStackTrace(e));
        } finally {
            try {
                if (br != null) {
                    br.close();
                }
                if (resourceAsStream != null) {
                    resourceAsStream.close();
                }
            } catch (IOException e) {
                log.info("FileReader or BufferedReader close error:{}", ExceptionUtils.getStackTrace(e));
            }

        }
    }

    @Override
    protected void prepare() {
        patterns = new HashMap<>();
        File file = new File(PATH);
        if (file.isDirectory()) {
            //读取当前目录下 patterns 资源目录下的正则规则
            loadPatterns(file);
        } else {
//            loadPatterns(new File(ClassLoader.getSystemResource("patterns").getFile()));
            File patterns = new File(Thread.currentThread().getContextClassLoader().getResource("patterns").getPath());
            if (patterns.isDirectory()) {
                //读取资源目录下的正则规则 主要是本地控制台启动的时候要用到
                loadPatterns(patterns);
            } else {
                //读取jar包内资源目录下的正则规则 主要是 使用单独jar包启动的时候要用到
                try {
                    ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
                    URL url = contextClassLoader.getResource("patterns/");
                    String jarPath = url.toString().substring(0, url.toString().indexOf("!/") + 2);
                    log.info("jarPath:{}", jarPath);
                    JarURLConnection jarCon = (JarURLConnection) new URL(jarPath).openConnection();
                    JarFile jarFile = jarCon.getJarFile();
                    Enumeration<JarEntry> jarEntrys = jarFile.entries();
                    while (jarEntrys.hasMoreElements()) {
                        JarEntry entry = jarEntrys.nextElement();
                        String name = entry.getName();
                        if (name.startsWith("BOOT-INF/classes/" + "patterns/") && !entry.isDirectory()) {
                            log.info("load pattern file:{}", name);
                            loadPatterns(contextClassLoader.getResourceAsStream(name));//open input stream in method
                        }
                    }
                } catch (IOException e) {
                    log.info("load jar inner patterns error:{}", ExceptionUtils.getStackTrace(e));
                }
            }
        }
        if (this.config.containsKey("pattern_paths")) {
            ((ArrayList<String>) this.config.get("pattern_paths")).forEach(path -> loadPatterns(new File(path)));
        }

        matches = new ArrayList<>();
        ((ArrayList<String>) this.config.get("match"))
                .forEach(matchString -> {
                    matchString = convertPattern(matchString);
                    byte[] bytes = matchString.getBytes(StandardCharsets.UTF_8);
                    Regex regex = new Regex(bytes, 0, bytes.length, Option.NONE, UTF8Encoding.INSTANCE);
                    matches.add(regex);
                });

        if (this.config.containsKey("encoding")) {
            this.encoding = (String) this.config.get("encoding");
        } else {
            this.encoding = "UTF8";
        }

        if (this.config.containsKey("lengthLimit")) {
            this.lengthLimit = Long.valueOf((Integer) this.config.get("lengthLimit"));
        } else {
            this.lengthLimit = MAX_LENGTH;
        }

        if (this.config.containsKey("src")) {
            this.src = (String) this.config.get("src");
        } else {
            this.src = "message";
        }

        if (this.config.containsKey("tag_on_failure")) {
            this.tagOnFailure = (String) this.config.get("tag_on_failure");
        } else {
            this.tagOnFailure = "grokfail";
        }
    }


    @Override
    protected Map<String, Object> filter(Map<String, Object> event) {
        if (!event.containsKey(this.src)) {
            return event;
        }
        boolean success = false;
        String input = ((String) event.get(this.src));
        byte[] bs;
        try {
            bs = input.getBytes(this.encoding);
        } catch (UnsupportedEncodingException e) {
            log.info("input.getBytes error, maybe wrong encoding? try do NOT use encoding.now encoding:{}", this.encoding);
            bs = input.getBytes();
        }

        //超长直接不处理了
        if (input.length() > this.lengthLimit) {
//            log.info("input length > {}", this.lengthLimit);
            return event;
        } else {
            long l = System.currentTimeMillis();
            for (Regex regex : this.matches) {
                try {
                    Matcher matcher = regex.matcher(bs);
                    int result = matcher.search(0, bs.length, Option.DEFAULT);
                    if (result != -1) {
                        success = true;
                        Region region = matcher.getEagerRegion();
                        for (Iterator<NameEntry> entryIterator = regex.namedBackrefIterator(); entryIterator.hasNext(); ) {
                            NameEntry entry = entryIterator.next();
                            int[] backRefs = entry.getBackRefs();
                            if (backRefs.length == 1) {
                                int number = backRefs[0];
                                int begin = region.beg[number];
                                int end = region.end[number];
                                if (begin != -1) {
                                    event.put(new String(entry.name, entry.nameP, entry.nameEnd - entry.nameP), new String(bs, begin, end - begin, this.encoding));
                                }
                            } else {
                                List<String> value = new ArrayList<>();
                                for (int number : backRefs) {
                                    int begin = region.beg[number];
                                    int end = region.end[number];
                                    if (begin != -1) {
                                        value.add(new String(bs, begin, end - begin, this.encoding));
                                    }
                                }
                                event.put(new String(entry.name, entry.nameP, entry.nameEnd
                                                                              - entry.nameP), value);
                            }
                        }
                        break;
                    }
                } catch (Exception e) {
                    log.info("grok failed:{}^^^exception:{}", JSON.toJSON(event), ExceptionUtils.getStackTrace(e));
                    success = false;
                }
            }

            this.postProcess(event, success);
            threadLocal.set(System.currentTimeMillis() - l + threadLocal.get());
            grokCount.incrementAndGet();
            if (grokCount.get() % 100_0000 == 0) {
                if (grokCount.get() > 1000_0000) {
                    grokCount.set(0);
                }
                log.info("Grok^^^100w消息处理耗时,{}", threadLocal.get());
                threadLocal.set(0L);
            }
            return event;
        }
    }

}
