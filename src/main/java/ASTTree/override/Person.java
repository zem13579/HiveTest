package ASTTree.override;

public class Person {
    public void function1(){
        System.out.println("this is function1 in Person !!!!");
    }

    public void startInPerson(){
        function1();
    }
}
