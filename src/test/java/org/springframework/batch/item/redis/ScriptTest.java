package org.springframework.batch.item.redis;

import javax.script.ScriptEngineFactory;
import javax.script.ScriptEngineManager;
import java.util.List;

public class ScriptTest {

    public static void main(String[] args) {
        ScriptEngineManager scriptEngineManager = new ScriptEngineManager();
        List<ScriptEngineFactory> engineFactories = scriptEngineManager.getEngineFactories();
        for (ScriptEngineFactory factory : engineFactories) {
            System.out.println("Engine: "+factory.getEngineName());
            System.out.println("   Language: "+factory.getLanguageName());
        }
    }
}
