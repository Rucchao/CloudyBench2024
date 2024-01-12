package com.cloudybench.util;

import umontreal.iro.lecuyer.probdist.PowerDist;

import java.util.Random;

public class PowerCDF {
    private PowerDist powerDist;

    public PowerCDF(double a, double b, double alpha) {
        powerDist = new PowerDist(a, b, alpha);
    }

    public PowerDist getPowerDist() {
        return powerDist;
    }

    public int getValue(Random random) {
        return (int) powerDist.inverseF(random.nextDouble());
    }

    public double getDouble(Random random) {
        return powerDist.inverseF(random.nextDouble());
    }
}