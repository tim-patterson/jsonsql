package jsonsql;

import com.oracle.svm.core.annotate.Substitute;
import com.oracle.svm.core.annotate.TargetClass;
import sun.misc.Signal;
import sun.misc.SignalHandler;


//@TargetClass(className = "org.jline.utils.Signals")
public final class JlineSignalsSub {
    //@Substitute
    private static Object doRegister(String name, Object handler) throws Exception {
        try {
            return Signal.handle(new Signal(name), (SignalHandler) handler);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }
}
