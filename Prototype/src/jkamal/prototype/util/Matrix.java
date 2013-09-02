/*************************************************************************
 *  Compilation:  javac Matrix.java
 *  Execution:    java Matrix
 *
 *  A bare-bones immutable data type for M-by-N matrices.
 *  
 *  Collected from : http://introcs.cs.princeton.edu/95linear/Matrix.java.html
 *
 *************************************************************************/

package jkamal.prototype.util;

import java.util.ArrayList;
import java.util.List;

final public class Matrix {
    private final int M;             // number of rows
    private final int N;             // number of columns
    private final double[][] item;   // M-by-N array
    
    // create M-by-N matrix of 0's
    public Matrix(int M, int N) {
        this.M = M;
        this.N = N;
        item = new double[M][N];
    }

    public int getM() {
		return M;
	}

	public int getN() {
		return N;
	}

	public double[][] getItem() {
		return item;
	}

	// create matrix based on 2d array
    public Matrix(double[][] data) {
        M = data.length;
        N = data[0].length;
        this.item = new double[M][N];
        for (int i = 0; i < M; i++)
            for (int j = 0; j < N; j++)
                    this.item[i][j] = data[i][j];
    }

    // copy constructor
    private Matrix(Matrix A) { this(A.item); }

    // create and return a random M-by-N matrix with values between 0 and 1
    public static Matrix random(int M, int N) {
        Matrix A = new Matrix(M, N);
        for (int i = 0; i < M; i++)
            for (int j = 0; j < N; j++)
                A.item[i][j] = Math.random();
        return A;
    }

    // create and return the N-by-N identity matrix
    public static Matrix identity(int N) {
        Matrix I = new Matrix(N, N);
        for (int i = 0; i < N; i++)
            I.item[i][i] = 1;
        return I;
    }

    // swap rows i and j
    public void swap_row(int i, int j) {
        double[] temp = item[i];
        item[i] = item[j];
        item[j] = temp;
    }

    // swap cols i and j
    public void swap_col(int i, int j) {
    	//double[] temp;
    	List<Double> temp = new ArrayList<Double>();
    	//System.out.println("-$-i|j("+i+", "+j+")");
    	for(int row = 0; row < M; row++) {
    		temp.add(item[row][i]);
    		//System.out.println("-@-["+item[row][i]+"]");
    	}
    	
    	for(int row = 0; row < M; row++) {
    		item[row][i] = item[row][j];
    	}
    	
    	for(int row = 0; row < M; row++) {
    		item[row][j] = temp.get(row);
    	}
    }
    
    // create and return the transpose of the invoking matrix
    public Matrix transpose() {
        Matrix A = new Matrix(N, M);
        for (int i = 0; i < M; i++)
            for (int j = 0; j < N; j++)
                A.item[j][i] = this.item[i][j];
        return A;
    }

    // return C = A + B
    public Matrix plus(Matrix B) {
        Matrix A = this;
        if (B.M != A.M || B.N != A.N) throw new RuntimeException("Illegal matrix dimensions.");
        Matrix C = new Matrix(M, N);
        for (int i = 0; i < M; i++)
            for (int j = 0; j < N; j++)
                C.item[i][j] = A.item[i][j] + B.item[i][j];
        return C;
    }


    // return C = A - B
    public Matrix minus(Matrix B) {
        Matrix A = this;
        if (B.M != A.M || B.N != A.N) throw new RuntimeException("Illegal matrix dimensions.");
        Matrix C = new Matrix(M, N);
        for (int i = 0; i < M; i++)
            for (int j = 0; j < N; j++)
                C.item[i][j] = A.item[i][j] - B.item[i][j];
        return C;
    }

    // does A = B exactly?
    public boolean eq(Matrix B) {
        Matrix A = this;
        if (B.M != A.M || B.N != A.N) throw new RuntimeException("Illegal matrix dimensions.");
        for (int i = 0; i < M; i++)
            for (int j = 0; j < N; j++)
                if (A.item[i][j] != B.item[i][j]) return false;
        return true;
    }

    // return C = A * B
    public Matrix times(Matrix B) {
        Matrix A = this;
        if (A.N != B.M) throw new RuntimeException("Illegal matrix dimensions.");
        Matrix C = new Matrix(A.M, B.N);
        for (int i = 0; i < C.M; i++)
            for (int j = 0; j < C.N; j++)
                for (int k = 0; k < A.N; k++)
                    C.item[i][j] += (A.item[i][k] * B.item[k][j]);
        return C;
    }


    // return x = A^-1 b, assuming A is square and has full rank
    public Matrix solve(Matrix rhs) {
        if (M != N || rhs.M != N || rhs.N != 1)
            throw new RuntimeException("Illegal matrix dimensions.");

        // create copies of the data
        Matrix A = new Matrix(this);
        Matrix b = new Matrix(rhs);

        // Gaussian elimination with partial pivoting
        for (int i = 0; i < N; i++) {

            // find pivot row and swap
            int max = i;
            for (int j = i + 1; j < N; j++)
                if (Math.abs(A.item[j][i]) > Math.abs(A.item[max][i]))
                    max = j;
            A.swap_row(i, max);
            b.swap_row(i, max);

            // singular
            if (A.item[i][i] == 0.0) throw new RuntimeException("Matrix is singular.");

            // pivot within b
            for (int j = i + 1; j < N; j++)
                b.item[j][0] -= b.item[i][0] * A.item[j][i] / A.item[i][i];

            // pivot within A
            for (int j = i + 1; j < N; j++) {
                double m = A.item[j][i] / A.item[i][i];
                for (int k = i+1; k < N; k++) {
                    A.item[j][k] -= A.item[i][k] * m;
                }
                A.item[j][i] = 0.0;
            }
        }

        // back substitution
        Matrix x = new Matrix(N, 1);
        for (int j = N - 1; j >= 0; j--) {
            double t = 0.0;
            for (int k = j + 1; k < N; k++)
                t += A.item[j][k] * x.item[k][0];
            x.item[j][0] = (b.item[j][0] - t) / A.item[j][j];
        }
        return x;
   
    }

    // print matrix to standard output
    public void print() {
        for (int i = 0; i < M; i++) {
            for (int j = 0; j < N; j++) 
                System.out.print(Math.round(item[i][j])+" ");
            System.out.println();
        }
    }
    
    // 
    public MatrixElement findMax(int pos) {
    	double max = 0;
    	MatrixElement e = new MatrixElement(0, 0 , 0);
    	
        for (int i = pos; i < M; i++) {
            for (int j = pos; j < N; j++) {
            	if(max < item[i][j]) {
            		max = item[i][j];
            		e.setRow_pos(i);
            		e.setCol_pos(j);
            		e.setValue(max);
            	}
            }            	            	            
        }
                    	
    	return e;
    }
}