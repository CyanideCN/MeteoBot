// SI_DLL.cpp: 定义 DLL 应用程序的导出函数。
//

#include "stdafx.h"

#include <iostream>
#include <vector>
#include <math.h>

using namespace std;

// Routine for calculating showalter index
// T in upper case means temperature in kelvin
// t in lower case means temperature in celsius

const double Rv = 461.5;
const double L0 = 2.5008e6;
const double cw = 4218;
const double Rd = 287;
const double cpd = 1004;

double vapor_pressure(double t) {
	const double a = 7.5;
	const double b = 237.3;
	return 6.11 * pow(10, (a * t) / (t + b));
}

double T_lcl(double T, double e) {
	return 2840 / (3.5 * log(T) - log(e) - 4.805) + 55;
}

double p_lcl(double p0, double T0, double T_lcl) {
	return p0 * pow(T_lcl / T0, cpd / Rd);
}

double condensation_latent_heat(double t) {
	double lc;
	lc = 4186.83 * (597.4 - 0.57 * t);
	return lc;
}

double r_s(double e_s, double p_lcl) {
	return 0.622 * e_s / (p_lcl - e_s);
}

double thse_Bolton(double T, double p, double T_lcl, double r) {
	double term_1, term_2;
	term_1 = T * pow(1000 / p, 0.2854 * (1 - 0.28 * r));
	term_2 = exp(r * (1 + 0.81 * r) * ((3376 / T_lcl) - 2.54));
	return term_1 * term_2;
}

double iterate(double t_ini, double t_delta, double thse850,
	double T_lcl, double p_lcl, int max_iter = 50, double epsilon = 1e-2) {
	double t_lower, t_trial, es, r, thse_trial, a, tmp;
	int i = 0;
	t_lower = t_ini;
	// First calculation
	es = vapor_pressure(t_lower);
	r = r_s(es, 500);
	thse_trial = thse_Bolton(t_trial + 273.16, 500, T_lcl, r);
	a = thse_trial - thse850;
	tmp = a; // Store valid computed result
	if (abs(a) < epsilon) {
		return t_lower;
	}
	for (i; i<max_iter; i++) {
		t_trial = t_lower + t_delta;
		es = vapor_pressure(t_trial);
		r = r_s(es, 500);
		thse_trial = thse_Bolton(t_trial + 273.16, 500, T_lcl, r);
		a = thse_trial - thse850;
		if (abs(a) < epsilon) {
			return t_trial;
		}
		else {
			if ((a * tmp) < 0) {
				t_delta *= 0.5;
			}
			else {
				if (abs(a) <= abs(tmp)) {
					t_lower = t_trial;
					tmp = a;
				}
				else {
					t_delta *= -1;
				}
			}
		}
	}
	return NAN;
}

double showalter_index(double t850, double td850, double t500) {
	double e, tlcl, plcl, r, thse_850, tp500;
	e = vapor_pressure(td850);
	tlcl = T_lcl(t850 + 273.16, e);
	plcl = p_lcl(850, t850 + 273.16, tlcl);
	r = r_s(e, plcl);
	thse_850 = thse_Bolton(t850 + 273.16, 850, tlcl, r);
	tp500 = iterate(-2, 10, thse_850, tlcl, plcl);
	return t500 - tp500;
}

double lifted_index(double t1000, double td1000, double t500) {
	double e, tlcl, plcl, r, thse_1000, tp500;
	e = vapor_pressure(td1000);
	tlcl = T_lcl(t1000 + 273.16, e);
	tlcl = T_lcl(t1000 + 273.16, e);
	plcl = p_lcl(1000, t1000 + 273.16, tlcl);
	r = r_s(e, plcl);
	thse_1000 = thse_Bolton(t1000 + 273.16, 1000, tlcl, r);
	tp500 = iterate(-2, 10, thse_1000, tlcl, plcl);
	return t500 - tp500;
}

double lifted_index_from_surface(double tsfc, double tdsfc, double t500, double psfc) {
	double e, tlcl, plcl, r, thse_sfc, tp500;
	if (psfc <= 500) {
		return NAN;
	}
	e = vapor_pressure(tdsfc);
	tlcl = T_lcl(tsfc + 273.16, e);
	tlcl = T_lcl(tsfc + 273.16, e);
	plcl = p_lcl(psfc, tsfc + 273.16, tlcl);
	r = r_s(e, plcl);
	thse_sfc = thse_Bolton(tsfc + 273.16, psfc, tlcl, r);
	tp500 = iterate(-2, 10, thse_sfc, tlcl, plcl);
	return t500 - tp500;
}