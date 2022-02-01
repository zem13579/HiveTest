package ASTTree.override;

public class Man extends Person{
    @Override
    public void function1() {
        System.out.println("this is function1 in man");
    }

    public void start(){
        super.startInPerson();
    }

    public static void main(String[] args) {
        new Man().start();
    }
}
