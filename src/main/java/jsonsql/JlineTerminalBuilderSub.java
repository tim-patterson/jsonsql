package jsonsql;

import com.oracle.svm.core.annotate.Substitute;
import com.oracle.svm.core.annotate.TargetClass;
import sun.misc.Signal;
import sun.misc.SignalHandler;

import java.lang.reflect.Method;
import java.util.Optional;


//@TargetClass(className = "org.jline.terminal.TerminalBuilder")
public final class JlineTerminalBuilderSub {
    //@Substitute
    private static String getParentProcessCommand() {
        try {
            Optional<ProcessHandle> parent = ProcessHandle.current().parent();
            // We can't use lambdas here... as that creates an unsubstituted class in the bytecode...
            if (parent.isPresent()) {
                return parent.get().info().command().orElse(null);
            } else {
                return null;
            }
        } catch (Throwable t) {
            return null;
        }
    }
}
